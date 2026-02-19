"""
Job Chain Tracker Module

Tracks rescheduled jobs by linking them via product serial number.
When a job is rescheduled, a new job is created with the same product serial.
This module detects and tracks these chains for reporting and alerts.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from supabase import Client


# Reschedule status keywords (case-insensitive matching)
RESCHEDULE_STATUSES = ['rescheduled', 'reschedule', 'resched']

# Completed status keywords
COMPLETED_STATUSES = ['delivered', 'complete', 'completed']


def is_rescheduled(status: str) -> bool:
    """Check if a status indicates the job was rescheduled."""
    if not status or pd.isna(status):
        return False
    return any(kw in str(status).lower() for kw in RESCHEDULE_STATUSES)


def is_completed(status: str) -> bool:
    """Check if a status indicates the job is complete."""
    if not status or pd.isna(status):
        return False
    return any(kw in str(status).lower() for kw in COMPLETED_STATUSES)


def detect_chains(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Groups jobs by product serial number to identify potential chains.
    
    Args:
        df: Processed DataFrame with Job_ID and Product_Serial columns
        
    Returns:
        Dict mapping product_serial -> list of job_ids (sorted by planned date)
    """
    if 'Product_Serial' not in df.columns or 'Job_ID' not in df.columns:
        print("⚠ Missing required columns for chain detection")
        return {}
    
    # Filter to jobs with valid serial numbers
    valid_serials = df[
        (df['Product_Serial'].notna()) & 
        (df['Product_Serial'] != '') & 
        (df['Product_Serial'] != 'nan') &
        (df['Product_Serial'] != 'None')
    ].copy()
    
    if valid_serials.empty:
        print("⚠ No jobs with valid product serial numbers found")
        return {}
    
    # Group by serial number
    chains = {}
    for serial, group in valid_serials.groupby('Product_Serial'):
        if len(group) > 1:
            # Multiple jobs with same serial - potential chain
            # Sort by planned date to establish sequence
            if 'Planned_Date' in group.columns:
                group = group.sort_values('Planned_Date', na_position='last')
            
            chains[str(serial)] = group['Job_ID'].tolist()
    
    print(f"✓ Detected {len(chains)} potential job chains")
    return chains


def calculate_chain_metrics(chain_jobs: pd.DataFrame) -> Dict:
    """
    Calculates metrics for a job chain.
    
    Args:
        chain_jobs: DataFrame containing all jobs in a chain
        
    Returns:
        Dict with chain metrics
    """
    metrics = {
        'total_jobs': len(chain_jobs),
        'reschedule_count': 0,
        'first_planned_date': None,
        'final_planned_date': None,
        'total_delay_days': 0,
        'current_status': None,
        'current_job_id': None
    }
    
    if chain_jobs.empty:
        return metrics
    
    # Count rescheduled jobs
    if 'Status' in chain_jobs.columns:
        metrics['reschedule_count'] = chain_jobs['Status'].apply(is_rescheduled).sum()
    
    # Get first and final planned dates
    if 'Planned_Date' in chain_jobs.columns:
        valid_dates = chain_jobs['Planned_Date'].dropna()
        if not valid_dates.empty:
            metrics['first_planned_date'] = valid_dates.min().date()
            metrics['final_planned_date'] = valid_dates.max().date()
            
            # Calculate total delay from first planned to now (if not completed)
            if metrics['first_planned_date']:
                days_since_first = (datetime.now().date() - metrics['first_planned_date']).days
                metrics['total_delay_days'] = max(0, days_since_first)
    
    # Get current (most recent) job status
    if 'Planned_Date' in chain_jobs.columns and 'Status' in chain_jobs.columns:
        # Sort by planned date descending to get most recent
        sorted_jobs = chain_jobs.sort_values('Planned_Date', ascending=False, na_position='last')
        latest_job = sorted_jobs.iloc[0]
        metrics['current_status'] = str(latest_job.get('Status', 'Unknown'))
        metrics['current_job_id'] = str(latest_job.get('Job_ID', ''))
    
    return metrics


class JobChainManager:
    """Manages job chain operations with Supabase backend."""
    
    def __init__(self, supabase_client: Client):
        """
        Initialize the chain manager.
        
        Args:
            supabase_client: Authenticated Supabase client
        """
        self.client = supabase_client
    
    def get_existing_chain(self, product_serial: str) -> Optional[Dict]:
        """
        Check if a chain already exists for this product serial.
        
        Args:
            product_serial: The product serial number to look up
            
        Returns:
            Chain record if exists, None otherwise
        """
        try:
            result = self.client.table('job_chains') \
                .select('*') \
                .eq('product_serial', product_serial) \
                .limit(1) \
                .execute()
            
            if result.data:
                return result.data[0]
            return None
        except Exception as e:
            print(f"⚠ Error checking existing chain: {e}")
            return None
    
    def create_chain(self, product_serial: str, carrier: str = None) -> Optional[str]:
        """
        Create a new job chain record.
        
        Args:
            product_serial: The product serial number
            carrier: Optional carrier code
            
        Returns:
            Chain ID if created, None on error
        """
        record = {
            'product_serial': product_serial,
            'carrier': carrier,
            'total_jobs': 0,
            'reschedule_count': 0,
            'created_at': datetime.now().isoformat()
        }
        
        try:
            result = self.client.table('job_chains').insert(record).execute()
            if result.data:
                return result.data[0].get('chain_id')
        except Exception as e:
            print(f"⚠ Error creating chain: {e}")
        return None
    
    def add_job_to_chain(self, chain_id: str, job_id: str, sequence_order: int,
                         status: str, planned_date, actual_date, 
                         delay_days: int = None, reschedule_reason: str = None) -> bool:
        """
        Add a job to an existing chain.
        
        Args:
            chain_id: UUID of the chain
            job_id: Job ID to add
            sequence_order: Order in the chain (1 = first/original)
            status: Job status
            planned_date: Planned delivery date
            actual_date: Actual delivery date (if any)
            delay_days: Days delayed from planned
            reschedule_reason: Reason for reschedule (if applicable)
            
        Returns:
            True if successful
        """
        record = {
            'chain_id': chain_id,
            'job_id': str(job_id),
            'sequence_order': sequence_order,
            'status': str(status) if status else None,
            'planned_date': planned_date.date().isoformat() if hasattr(planned_date, 'date') else planned_date,
            'actual_date': actual_date.isoformat() if actual_date and pd.notna(actual_date) else None,
            'delay_days': int(delay_days) if pd.notna(delay_days) else None,
            'reschedule_reason': reschedule_reason,
            'linked_at': datetime.now().isoformat()
        }
        
        try:
            self.client.table('job_chain_links').upsert(
                record, 
                on_conflict='chain_id,job_id'
            ).execute()
            return True
        except Exception as e:
            print(f"⚠ Error adding job to chain: {e}")
            return False
    
    def update_chain_metadata(self, chain_id: str, metrics: Dict) -> bool:
        """
        Update chain metadata with calculated metrics.
        
        Args:
            chain_id: UUID of the chain
            metrics: Dict of metrics from calculate_chain_metrics()
            
        Returns:
            True if successful
        """
        update_data = {
            'total_jobs': metrics.get('total_jobs', 0),
            'reschedule_count': metrics.get('reschedule_count', 0),
            'first_planned_date': metrics.get('first_planned_date'),
            'final_planned_date': metrics.get('final_planned_date'),
            'total_delay_days': metrics.get('total_delay_days', 0),
            'current_status': metrics.get('current_status'),
            'current_job_id': metrics.get('current_job_id'),
            'updated_at': datetime.now().isoformat()
        }
        
        try:
            self.client.table('job_chains') \
                .update(update_data) \
                .eq('chain_id', chain_id) \
                .execute()
            return True
        except Exception as e:
            print(f"⚠ Error updating chain metadata: {e}")
            return False
    
    def get_active_chains(self, min_reschedules: int = 0) -> List[Dict]:
        """
        Get all active chains (not completed).
        
        Args:
            min_reschedules: Minimum reschedule count filter
            
        Returns:
            List of chain records
        """
        try:
            query = self.client.table('job_chains') \
                .select('*') \
                .not_.in_('current_status', COMPLETED_STATUSES)
            
            if min_reschedules > 0:
                query = query.gte('reschedule_count', min_reschedules)
            
            result = query.order('reschedule_count', desc=True).execute()
            return result.data or []
        except Exception as e:
            print(f"⚠ Error fetching active chains: {e}")
            return []
    
    def get_chain_links(self, chain_id: str) -> List[Dict]:
        """
        Get all jobs in a chain.
        
        Args:
            chain_id: UUID of the chain
            
        Returns:
            List of job link records
        """
        try:
            result = self.client.table('job_chain_links') \
                .select('*') \
                .eq('chain_id', chain_id) \
                .order('sequence_order') \
                .execute()
            return result.data or []
        except Exception as e:
            print(f"⚠ Error fetching chain links: {e}")
            return []


def process_job_chains(df: pd.DataFrame, supabase_client: Client) -> Dict:
    """
    Main function to detect and store job chains.
    Called during daily import.
    
    Args:
        df: Processed DataFrame with job data
        supabase_client: Authenticated Supabase client
        
    Returns:
        Dict with processing statistics
    """
    stats = {
        'chains_processed': 0,
        'new_chains_created': 0,
        'jobs_linked': 0,
        'errors': 0
    }
    
    if df.empty:
        print("⚠ Empty DataFrame, skipping chain processing")
        return stats
    
    # Check required columns
    required = ['Job_ID', 'Product_Serial']
    missing = [col for col in required if col not in df.columns]
    if missing:
        print(f"⚠ Missing required columns for chain processing: {missing}")
        return stats
    
    # Filter to jobs with valid serial numbers
    valid_jobs = df[
        (df['Product_Serial'].notna()) & 
        (df['Product_Serial'] != '') & 
        (df['Product_Serial'] != 'nan') &
        (df['Product_Serial'] != 'None')
    ].copy()
    
    if valid_jobs.empty:
        print("⚠ No jobs with valid product serial numbers")
        return stats
    
    manager = JobChainManager(supabase_client)
    
    # Group by product serial
    for serial, group in valid_jobs.groupby('Product_Serial'):
        serial_str = str(serial)
        
        # Skip single jobs (no chain needed)
        if len(group) == 1:
            continue
        
        try:
            # Check for existing chain
            existing = manager.get_existing_chain(serial_str)
            
            if existing:
                chain_id = existing['chain_id']
            else:
                # Create new chain
                carrier = group.iloc[0].get('Carrier', None) if 'Carrier' in group.columns else None
                chain_id = manager.create_chain(serial_str, carrier)
                if chain_id:
                    stats['new_chains_created'] += 1
                else:
                    stats['errors'] += 1
                    continue
            
            # Sort by planned date for sequence
            if 'Planned_Date' in group.columns:
                group = group.sort_values('Planned_Date', na_position='last')
            
            # Add each job to the chain
            for seq_order, (_, job) in enumerate(group.iterrows(), start=1):
                success = manager.add_job_to_chain(
                    chain_id=chain_id,
                    job_id=str(job.get('Job_ID', '')),
                    sequence_order=seq_order,
                    status=str(job.get('Status', '')),
                    planned_date=job.get('Planned_Date'),
                    actual_date=job.get('Actual_Date'),
                    delay_days=job.get('Delay_Days'),
                    reschedule_reason=None  # Could be extracted from notes if available
                )
                if success:
                    stats['jobs_linked'] += 1
            
            # Update chain metadata
            metrics = calculate_chain_metrics(group)
            manager.update_chain_metadata(chain_id, metrics)
            
            stats['chains_processed'] += 1
            
        except Exception as e:
            print(f"⚠ Error processing chain for serial {serial_str}: {e}")
            stats['errors'] += 1
    
    print(f"✓ Chain processing complete: {stats['chains_processed']} chains, "
          f"{stats['jobs_linked']} jobs linked, {stats['new_chains_created']} new")
    
    return stats


def get_chain_alerts(supabase_client: Client) -> List[Dict]:
    """
    Get chains that meet alert criteria.
    
    Alert Rules:
    - Critical: 3+ reschedules
    - Warning: 2 reschedules OR 14+ days since first planned
    
    Args:
        supabase_client: Authenticated Supabase client
        
    Returns:
        List of alert records with severity
    """
    alerts = []
    
    try:
        # Get active chains
        result = supabase_client.table('job_chains') \
            .select('*') \
            .not_.in_('current_status', COMPLETED_STATUSES) \
            .or_('reschedule_count.gte.2,total_delay_days.gte.14') \
            .order('reschedule_count', desc=True) \
            .execute()
        
        for chain in (result.data or []):
            reschedule_count = chain.get('reschedule_count', 0)
            delay_days = chain.get('total_delay_days', 0)
            
            # Determine severity
            if reschedule_count >= 3:
                severity = 'critical'
                message = f"Product rescheduled {reschedule_count} times - investigate carrier"
            elif reschedule_count >= 2:
                severity = 'warning'
                message = f"Product rescheduled {reschedule_count} times"
            elif delay_days >= 14:
                severity = 'warning'
                message = f"Product delayed {delay_days} days from original planned date"
            else:
                continue  # Doesn't meet alert threshold
            
            alerts.append({
                'chain_id': chain.get('chain_id'),
                'product_serial': chain.get('product_serial'),
                'carrier': chain.get('carrier'),
                'reschedule_count': reschedule_count,
                'total_delay_days': delay_days,
                'current_status': chain.get('current_status'),
                'current_job_id': chain.get('current_job_id'),
                'severity': severity,
                'message': message
            })
        
    except Exception as e:
        print(f"⚠ Error fetching chain alerts: {e}")
    
    return alerts


# For testing
if __name__ == "__main__":
    print("Job Chain Tracker Module")
    print("=" * 40)
    print("\nFunctions available:")
    print("  - detect_chains(df)")
    print("  - calculate_chain_metrics(chain_jobs)")
    print("  - process_job_chains(df, supabase_client)")
    print("  - get_chain_alerts(supabase_client)")
    print("\nClasses:")
    print("  - JobChainManager(supabase_client)")