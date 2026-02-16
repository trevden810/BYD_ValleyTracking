"""
Email Report Generator Module

Generates professional HTML email reports with action items and job lists.
"""

from jinja2 import Environment, FileSystemLoader
import pandas as pd
from datetime import datetime
import os
from typing import Dict, List
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def get_overdue_jobs(df: pd.DataFrame) -> List[Dict]:
    """
    Gets overdue arrivals (Watchlist).
    Planned Date < Today AND Actual Date IS NULL
    """
    today = datetime.now().date()
    # Ensure Planned_Date is datetime
    if 'Planned_Date' not in df.columns or 'Actual_Date' not in df.columns:
        return []
        
    mask = (df['Planned_Date'].dt.date < today) & (df['Actual_Date'].isna())
    overdue_df = df[mask].copy()
    
    if overdue_df.empty:
        return []

    # Calculate days overdue
    overdue_df['Days_Overdue'] = (pd.to_datetime(today) - overdue_df['Planned_Date'].dt.normalize()).dt.days
    
    # Sort by days overdue (descending)
    overdue_df = overdue_df.sort_values('Days_Overdue', ascending=False)
    
    results = []
    for _, row in overdue_df.iterrows():
        results.append({
            'job_id': row.get('Job_ID', 'N/A'),
            'carrier': row.get('Carrier', 'N/A'),
            'planned_date': row.get('Planned_Date').strftime('%Y-%m-%d') if pd.notna(row.get('Planned_Date')) else 'N/A',
            'days_overdue': int(row.get('Days_Overdue', 0)),
            'market': row.get('Market', 'N/A')
        })
    return results


def get_ready_for_routing_jobs(df: pd.DataFrame) -> List[Dict]:
    """
    Gets jobs ready for routing.
    Actual Date IS NOT NULL AND Is_Routed IS FALSE
    """
    if 'Actual_Date' not in df.columns or 'Is_Routed' not in df.columns:
        return []
        
    mask = (df['Actual_Date'].notna()) & (df['Is_Routed'] == False)
    routing_df = df[mask].copy()
    
    if routing_df.empty:
        return []
        
    # Sort by Actual Date (descending - most recent arrival first)
    routing_df = routing_df.sort_values('Actual_Date', ascending=False)
    
    results = []
    for _, row in routing_df.iterrows():
        results.append({
            'job_id': row.get('Job_ID', 'N/A'),
            'carrier': row.get('Carrier', 'N/A'),
            'actual_date': row.get('Actual_Date').strftime('%Y-%m-%d %H:%M') if pd.notna(row.get('Actual_Date')) else 'N/A',
            'market': row.get('Market', 'N/A'),
            'status': row.get('Status', 'N/A')
        })
    return results


def generate_html_report(df: pd.DataFrame, kpis: Dict, trends: Dict, deltas: Dict = None) -> str:
    """
    Generates HTML email report from template.
    
    Args:
        df: Processed DataFrame
        kpis: KPI dictionary
        trends: Trend indicators
        deltas: Daily activity deltas (new jobs, arrivals, etc.)
        
    Returns:
        HTML string
    """
    if deltas is None:
        deltas = {'new_jobs': [], 'new_arrivals': [], 'new_deliveries': [], 'new_overdue': []}
        
    # Helper function for trend CSS class
    def trend_class(trend_str: str) -> str:
        if not trend_str or trend_str == '→':
            return 'trend-stable'
        if 'Improved' in str(trend_str):
            return 'trend-up'
        elif 'Worsened' in str(trend_str):
            return 'trend-down'
        elif '↑' in str(trend_str):
            if 'Improved' in str(trend_str):
                return 'trend-up'
            else:
                return 'trend-down'
        elif '↓' in str(trend_str):
            if 'Improved' in str(trend_str):
                return 'trend-up'
            else:
                return 'trend-down'
        else:
            return 'trend-stable'
    
    # Setup Jinja2
    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
    env = Environment(loader=FileSystemLoader(template_dir))
    
    # Register custom filter
    env.filters['trend_class'] = trend_class
    
    template = env.get_template('email_report.html')
    
    # Get Action Items
    overdue_jobs = get_overdue_jobs(df)
    ready_routing_jobs = get_ready_for_routing_jobs(df)
    
    # Render template
    html = template.render(
        report_date=datetime.now().strftime('%B %d, %Y'),
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        kpis=kpis,
        trends=trends,
        deltas=deltas,
        overdue_jobs=overdue_jobs,
        ready_routing_jobs=ready_routing_jobs
    )
    
    return html


def send_email_outlook(html_content: str, recipients: List[str], subject: str = None, display_only: bool = True):
    """
    Sends HTML email using local Outlook application (Windows only).
    """
    if subject is None:
        subject = f"BYD/Valley Tracking Report - {datetime.now().strftime('%Y-%m-%d')}"
    
    try:
        import win32com.client
    except ImportError:
        raise ImportError("pywin32 not installed. Run: pip install pywin32")
    
    try:
        # Create Outlook application object
        outlook = win32com.client.Dispatch('Outlook.Application')
        mail = outlook.CreateItem(0)  # 0 = MailItem
        
        # Set recipients
        mail.To = '; '.join(recipients)
        
        # Set subject
        mail.Subject = subject
        
        # Set HTML body
        mail.HTMLBody = html_content
        
        # Display or Send
        if display_only:
            mail.Display(False)  # False = non-modal
            print(f"✓ Email draft opened in Outlook for review (to {len(recipients)} recipient(s))")
        else:
            mail.Send()
            print(f"✓ Email sent via Outlook to {len(recipients)} recipient(s)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error with Outlook email: {e}")
        raise


def send_email(html_content: str, recipients: List[str], subject: str = None, method: str = 'auto', display_only: bool = True):
    """
    Sends HTML email.
    """
    if subject is None:
        subject = f"BYD/Valley Tracking Report - {datetime.now().strftime('%Y-%m-%d')}"
    
    # Auto mode: Try Outlook first (Windows only), fall back to SMTP
    if method == 'auto':
        try:
            return send_email_outlook(html_content, recipients, subject, display_only)
        except Exception as e:
            print(f"⚠ Outlook method failed ({e}), trying SMTP...")
            method = 'smtp'
    
    # Outlook method
    if method == 'outlook':
        return send_email_outlook(html_content, recipients, subject, display_only)
    
    # SMTP method
    if method == 'smtp':
        # Get SMTP credentials from environment
        smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', 587))
        smtp_user = os.getenv('SMTP_USER')
        smtp_password = os.getenv('SMTP_PASSWORD')
        
        if not smtp_user or not smtp_password:
            raise ValueError("SMTP_USER and SMTP_PASSWORD must be set in .env")
        
        # Create message
        msg = MIMEMultipart('related')
        msg['Subject'] = subject
        msg['From'] = smtp_user
        msg['To'] = ', '.join(recipients)
        
        # Attach HTML
        html_part = MIMEText(html_content, 'html')
        msg.attach(html_part)
        
        # Send email
        try:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            server.quit()
            print(f"✓ Email sent via SMTP to {len(recipients)} recipient(s)")
            return True
        except Exception as e:
            print(f"❌ Error sending email via SMTP: {e}")
            raise


if __name__ == "__main__":
    print("Email generator module loaded. Use generate_html_report() or send_email().")
