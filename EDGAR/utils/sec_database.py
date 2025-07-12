#!/usr/bin/env python3
"""
SQLite database management for SEC EDGAR filings
"""
import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from utils.config import DATABASE_PATH, BASE_DATA_DIR

class SECDatabase:
    def __init__(self):
        self.db_path = DATABASE_PATH
        self.init_database()
        self.logger = logging.getLogger(__name__)
        self._downloaded_cache = {}
        self._cache_size_limit = 10000
    
    def init_database(self):
        """Create database and tables if they don't exist"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS filings (
                    filing_id TEXT PRIMARY KEY,
                    cik TEXT NOT NULL,
                    accession_number TEXT NOT NULL,
                    form_type TEXT NOT NULL,
                    company_name TEXT,
                    ticker TEXT,
                    fiscal_year TEXT,
                    fiscal_period TEXT,
                    filing_date TEXT,
                    period_end_date TEXT,
                    file_path TEXT,
                    file_count INTEGER DEFAULT 0,
                    total_size INTEGER DEFAULT 0,
                    download_status TEXT DEFAULT 'completed',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for fast lookups
            conn.execute('CREATE INDEX IF NOT EXISTS idx_cik ON filings(cik)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_accession ON filings(accession_number)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_filing_date ON filings(filing_date)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_form_type ON filings(form_type)')
            
            conn.commit()
    
    def is_filing_downloaded(self, cik: str, accession_number: str) -> bool:
        """Check if filing exists in database"""
        filing_id = f"{cik}_{accession_number}"
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT 1 FROM filings WHERE filing_id = ?', 
                (filing_id,)
            )
            return cursor.fetchone() is not None
    
    def add_filing(self, filing_data: Dict) -> bool:
        """Add new filing record to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO filings 
                    (filing_id, cik, accession_number, form_type, company_name, ticker,
                     fiscal_year, fiscal_period, filing_date, period_end_date, 
                     file_path, file_count, total_size, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    filing_data['filing_id'],
                    filing_data['cik'],
                    filing_data['accession_number'],
                    filing_data['form_type'],
                    filing_data.get('company_name', ''),
                    filing_data.get('ticker', ''),
                    filing_data.get('fiscal_year', ''),
                    filing_data.get('fiscal_period', ''),
                    filing_data.get('filing_date', ''),
                    filing_data.get('period_end_date', ''),
                    filing_data.get('file_path', ''),
                    filing_data.get('file_count', 0),
                    filing_data.get('total_size', 0),
                    datetime.now().isoformat()
                ))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Error adding filing {filing_data.get('filing_id')}: {e}")
            return False
    
    def get_downloaded_filings(self, cik: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get list of downloaded filings with optional filters"""
        query = 'SELECT * FROM filings WHERE 1=1'
        params = []
        
        if cik:
            query += ' AND cik = ?'
            params.append(cik)
        
        if start_date:
            query += ' AND filing_date >= ?'
            params.append(start_date)
            
        if end_date:
            query += ' AND filing_date <= ?'
            params.append(end_date)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_stats(self) -> Dict:
        """Get database statistics - FIXED VERSION"""
        with sqlite3.connect(self.db_path) as conn:
            # Get overall totals first
            cursor = conn.execute('''
                SELECT 
                    COUNT(*) as total_filings,
                    COUNT(DISTINCT cik) as unique_companies,
                    COUNT(DISTINCT fiscal_year) as years_covered
                FROM filings
            ''')
            
            row = cursor.fetchone()
            total_filings = row[0]
            unique_companies = row[1] 
            years_covered = row[2]
            
            # Get form type breakdown separately
            cursor = conn.execute('''
                SELECT form_type, COUNT(*) as count
                FROM filings 
                GROUP BY form_type
                ORDER BY count DESC
            ''')
            
            form_stats = {}
            for row in cursor.fetchall():
                form_stats[row[0]] = row[1]
            
            return {
                'total_filings': total_filings,
                'unique_companies': unique_companies,
                'years_covered': years_covered,
                'form_types': form_stats
            }
    
    
    def are_filings_downloaded_batch(self, filing_ids: List[str]) -> Dict[str, bool]:
        """
        ULTRA-FAST: Check multiple filings in a single database query
        Instead of N queries, this does 1 query for N filings
        """
        if not filing_ids:
            return {}
        
        # Check cache first
        results = {}
        uncached_ids = []
        
        for filing_id in filing_ids:
            if filing_id in self._downloaded_cache:
                results[filing_id] = self._downloaded_cache[filing_id]
            else:
                uncached_ids.append(filing_id)
        
        # Query database for uncached IDs only
        if uncached_ids:
            # Use IN clause for batch lookup
            placeholders = ','.join(['?' for _ in uncached_ids])
            query = f'SELECT filing_id FROM filings WHERE filing_id IN ({placeholders})'
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(query, uncached_ids)
                downloaded_ids = set(row[0] for row in cursor.fetchall())
            
            # Update results and cache
            for filing_id in uncached_ids:
                is_downloaded = filing_id in downloaded_ids
                results[filing_id] = is_downloaded
                
                # Update cache with size limit
                if len(self._downloaded_cache) < self._cache_size_limit:
                    self._downloaded_cache[filing_id] = is_downloaded
        
        return results

    def get_downloaded_companies_years(self) -> Dict[str, set]:
        """
       Get all companies and years that have ANY downloaded filings
        This allows skipping entire companies without individual filing checks
        
        Returns:
            Dict mapping CIK -> set of years with downloaded filings
        """
        query = '''
        SELECT cik, fiscal_year 
        FROM filings 
        WHERE fiscal_year != '' AND fiscal_year IS NOT NULL
        '''
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query)
            
            company_years = {}
            for cik, year in cursor.fetchall():
                if cik not in company_years:
                    company_years[cik] = set()
                company_years[cik].add(year)
        
        return company_years
    
    def get_downloaded_filings_by_cik_year(self, cik: str, year: str) -> set[str]:
        """
        ULTRA-FAST: Get all downloaded filing accession numbers for a specific company/year
        """
        query = '''
        SELECT accession_number, form_type, fiscal_period
        FROM filings 
        WHERE cik = ? AND fiscal_year = ?
        '''
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, (cik, year))
            result = {}
            for row in cursor.fetchall():
                accession_number = row[0]
                result[accession_number] = {
                    'form_type': row[1],
                    'fiscal_period': row[2]
                       
                }
            return result
        

    def get_optimized_connection(self):
        """Get SQLite connection with performance optimizations"""
        conn = sqlite3.connect(self.db_path)
        conn.execute('PRAGMA journal_mode = WAL')
        conn.execute('PRAGMA synchronous = NORMAL') 
        conn.execute('PRAGMA cache_size = -64000')  # 64MB
        conn.execute('PRAGMA temp_store = MEMORY')
        conn.execute('PRAGMA mmap_size = 268435456')  # 256MB
        return conn

    def get_fiscal_info(self, cik: str, accession_number: str) -> Optional[str]:
        """Get fiscal year for a specific filing"""
        filing_id = f"{cik}_{accession_number}"
        query = '''
        SELECT fiscal_year, fiscal_period, form_type, ticker
        FROM filings
        WHERE filing_id = ?'''
        with self.get_optimized_connection() as conn:
            cursor = conn.execute(query, (filing_id,))
            row = cursor.fetchone()
            if row:
                fiscal_year, fiscal_period, form_type, ticker = row
                if fiscal_year and fiscal_period and form_type.startswith('10-'):
                    return fiscal_year, fiscal_period, form_type, ticker

    def batch_check_downloaded(self, filing_ids: List[str]) -> set[str]:
        """Ultra-fast batch check for downloaded filings"""
        if not filing_ids:
            return set()
        
        placeholders = ','.join(['?' for _ in filing_ids])
        query = f'SELECT filing_id FROM filings WHERE filing_id IN ({placeholders})'
        
        with self.get_optimized_connection() as conn:
            cursor = conn.execute(query, filing_ids)
            return set(row[0] for row in cursor.fetchall())

    def delete_filing_record(self, cik: str, accession_number:str):
        query = '''
        DELETE FROM filings 
        WHERE filing_id = ?
        '''
        params = f"{cik}_{accession_number}"
        
        # Execute the deletion
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query,(params,))
            deleted_count = cursor.rowcount
            conn.commit()
            
            if deleted_count > 0:
                self.logger.info(f" Deleted {deleted_count} filing record(s) for {cik}")
            else:
                self.logger.warning(f" No records found to delete for {cik}")
            
            return deleted_count
    

if __name__ == "__main__":
    # Test database creation and basic operations
    logging.basicConfig(level=logging.INFO)
    
    db = SECDatabase()
    stats = db.get_downloaded_filings_by_cik_year('0000002098','2025')

    print(f"Database stats: {stats}")
    