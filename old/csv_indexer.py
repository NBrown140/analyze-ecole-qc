import json
from pathlib import Path
import os
import pickle
from datetime import datetime
import hashlib

import chardet
import pandas as pd


class AdvancedCsvIndexer:
    def __init__(self, csv_path, id_column='id', index_dir=None):
        """
        Initialize the indexer with enhanced features.
        
        Args:
            csv_path (str): Path to the CSV file
            id_column (str): Name of the ID column (default: 'id')
            index_dir (str): Directory to store index files (default: same as CSV)
        """
        self.csv_path = Path(csv_path)
        self.id_column = id_column
        self.index_dir = Path(index_dir) if index_dir else self.csv_path.parent
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.index_dir / f"{self.csv_path.stem}_index.pkl"
        self.metadata_path = self.index_dir / f"{self.csv_path.stem}_metadata.json"
        self.encoding = self._detect_encoding()

        
    def _detect_encoding(self):
        """Detect the file encoding using chardet"""
        print("Detecting file encoding...")
        # Read a sample of the file to detect encoding
        with open(self.csv_path, 'rb') as file:
            raw_data = file.read(min(1024*1024, os.path.getsize(self.csv_path)))  # Read up to 1MB
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            print(f"Detected encoding: {encoding} (confidence: {confidence:.2%})")
            return encoding
    
    def _get_file_hash(self):
        """Calculate SHA-256 hash of the first 1MB of the file"""
        with open(self.csv_path, 'rb') as f:
            return hashlib.sha256(f.read(1024 * 1024)).hexdigest()
    
    def _save_metadata(self, row_count, creation_time):
        """Save index metadata"""
        metadata = {
            'csv_path': str(self.csv_path),
            'id_column': self.id_column,
            'row_count': row_count,
            'creation_time': creation_time,
            'file_hash': self._get_file_hash(),
            'encoding': self.encoding
        }
        with open(self.metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f)
    
    def is_index_valid(self):
        """Check if index is valid and up-to-date"""
        if not self.index_path.exists() or not self.metadata_path.exists():
            return False
            
        with open(self.metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
            
        return (metadata['file_hash'] == self._get_file_hash() and
                metadata['csv_path'] == str(self.csv_path) and
                metadata['id_column'] == self.id_column and
                metadata.get('encoding') == self.encoding)
    
    def create_index(self, chunksize=10000, dtype=None):
        """
        Create an optimized index file mapping IDs to file positions.
        
        Args:
            chunksize (int): Number of rows to process at a time
            dtype (dict): Dictionary of column data types for optimized reading
        """
        index = {}
        row_count = 0
        
        print(f"Creating index for {self.csv_path}...")
        start_time = datetime.now()
        
        try:
            # First verify we can read the file with pandas
            test_chunk = pd.read_csv(self.csv_path, nrows=1, encoding=self.encoding)
            if self.id_column not in test_chunk.columns:
                raise ValueError(f"ID column '{self.id_column}' not found in CSV. "
                               f"Available columns: {', '.join(test_chunk.columns)}")
            
            with open(self.csv_path, 'rb') as f:
                header = f.readline()
                header_length = len(header)
                
                chunks = pd.read_csv(self.csv_path, 
                                   chunksize=chunksize, 
                                   dtype=dtype, 
                                   encoding=self.encoding)
                current_pos = header_length
                
                for chunk_num, chunk in enumerate(chunks):
                    row_count += len(chunk)
                    
                    # Convert chunk to CSV string with the same encoding as the file
                    chunk_str = chunk.to_csv(index=False, header=False, encoding=self.encoding)
                    row_lengths = chunk_str.split('\n')
                    row_lengths = [len(row.encode(self.encoding)) + 1 for row in row_lengths[:-1]]
                    
                    # Update index
                    for idx, row in chunk.iterrows():
                        id_val = str(row[self.id_column])
                        if id_val not in index:
                            index[id_val] = []
                        index[id_val].append(current_pos)
                        current_pos += row_lengths[idx - chunk.index[0]]
                    
                    if (chunk_num + 1) % 10 == 0:
                        print(f"Processed {row_count:,} rows...")
            
            # Save index using pickle
            print(f'Saving index to: {self.index_path}')
            with open(self.index_path, 'wb') as f:
                pickle.dump(index, f)
            
            # Save metadata
            self._save_metadata(row_count, datetime.now().isoformat())
            
            print(f"Index creation completed in {datetime.now() - start_time}")
            print(f"Total rows indexed: {row_count:,}")
            print(f"Unique IDs found: {len(index):,}")
            
        except Exception as e:
            print(f"Error creating index: {str(e)}")
            # Clean up partial index files if they exist
            if self.index_path.exists():
                self.index_path.unlink()
            if self.metadata_path.exists():
                self.metadata_path.unlink()
            raise
    
    def load_rows_by_id(self, target_id, dtype=None):
        """
        Load rows matching the target ID using the index file.
        
        Args:
            target_id (str): The ID to look up
            dtype (dict): Dictionary of column data types for optimized reading
            
        Returns:
            pandas.DataFrame: DataFrame containing all matching rows
        """
        target_id = str(target_id)
        
        if not self.is_index_valid():
            raise FileNotFoundError("Index is missing or invalid. Please create/recreate index.")
        
        # Load index using pickle
        with open(self.index_path, 'rb') as f:
            index = pickle.load(f)
        
        if target_id not in index:
            return pd.read_csv(self.csv_path, nrows=0, dtype=dtype, encoding=self.encoding)
        
        positions = index[target_id]
        rows = []
        
        with open(self.csv_path, 'r', encoding=self.encoding) as f:
            header = f.readline().strip().split(',')
            for pos in positions:
                f.seek(pos)
                row = f.readline().strip().split(',')
                rows.append(dict(zip(header, row)))
        
        return pd.DataFrame(rows).astype(dtype) if dtype else pd.DataFrame(rows)

# Example usage:
if __name__ == "__main__":
    try:
        # Initialize indexer
        indexer = AdvancedCsvIndexer(
            'your_large_file.csv',
            id_column='id',
            index_dir='indexes'
        )
        
        # Define data types (optional but recommended for large files)
        dtypes = {
            'id': str,
            'value': float,
            'category': 'category'
        }
        
        # Create index if needed
        if not indexer.is_index_valid():
            indexer.create_index(dtype=dtypes)
        
        # Perform fast lookups
        result_df = indexer.load_rows_by_id('123', dtype=dtypes)
        print(result_df)
        
    except Exception as e:
        print(f"Error: {str(e)}")