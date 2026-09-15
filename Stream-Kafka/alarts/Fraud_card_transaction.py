import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

# Fetch Gmail credentials from secrets (outside the handler for serialization compatibility)
gmail_sender = "sutradharj61@gmail.com"  
gmail_password = dbutils.secrets.get(scope="finguard-scope", key="gmail_api_key")


def send_urgent_fraud_alert(to_email, customer_name, transaction_id, card_number, amount, 
                            transaction_time, customer_city, watchlist_reason):
    """
    Send an urgent fraud alert email for watchlist-matched transactions
    """
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = gmail_sender
        msg['To'] = to_email
        msg['Subject'] = f'🚨 URGENT: Fraudulent Card Activity Detected - Immediate Action Required'

        # Email body with urgent tone
        body = f"""
        URGENT FRAUD ALERT
        
        Dear {customer_name},

        🚨 CRITICAL SECURITY ALERT 🚨

        We have detected a fraudulent transaction on your account. Your card has been flagged 
        in our security watchlist system and requires IMMEDIATE action.

        FRAUD TRANSACTION DETAILS:
        ========================
        Transaction ID: {transaction_id}
        Card Number: ****{card_number[-4:]}
        Amount: {amount}
        Transaction Time: {transaction_time}
        Customer Location: {customer_city}
        
        SECURITY ALERT REASON:
        {watchlist_reason}
        
        ⚠️ IMMEDIATE ACTION REQUIRED ⚠️
        
        1. If you DID NOT authorize this transaction:
           - Your card has been automatically flagged for review
           - Contact our fraud department IMMEDIATELY at 1-800-XXX-XXXX
           - Do NOT use this card for any further transactions
           
        2. If you DID authorize this transaction:
           - Contact us immediately to verify and remove the fraud flag
           - You may need to provide additional verification
        
        For your security, we recommend:
        • Reviewing all recent transactions on your account
        • Changing your online banking passwords
        • Monitoring your account for any suspicious activity
        
        DO NOT REPLY TO THIS EMAIL. Contact our fraud hotline directly.
        
        Fraud Prevention Team
        Security Operations Center
        Available 24/7
        """

        msg.attach(MIMEText(body, 'plain'))

        # Connect to Gmail SMTP server and send email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(gmail_sender, gmail_password)
            server.send_message(msg)
        
        return True
    except Exception as e:
        print(f"❌ Failed to send urgent fraud alert to {to_email}: {str(e)}")
        return False


@dp.foreach_batch_sink(name="fraud_card_email_sink")
def fraud_card_email_sink(df: DataFrame, batch_id: int):
    """
    ForEachBatch sink handler that sends urgent fraud alert emails 
    for watchlist-matched transactions
    """
    # Collect the fraud transactions in this micro-batch
    fraud_transactions = df.collect()
    
    print(f"\n{'='*60}")
    print(f"📧 Processing batch {batch_id}: {len(fraud_transactions)} fraud alert(s) detected")
    print(f"{'='*60}\n")
    
    # Send urgent email for each fraud transaction
    for row in fraud_transactions:
        customer_email = row.customer_email
        customer_name = row.customer_name
        transaction_id = row.transaction_id
        card_number = row.card_number
        amount = row.amount
        transaction_time = row.transaction_timestamp
        customer_city = row.customer_city
        watchlist_reason = row.watchlist_reason
        
        # Send the urgent fraud alert
        success = send_urgent_fraud_alert(
            to_email=customer_email,
            customer_name=customer_name,
            transaction_id=transaction_id,
            card_number=card_number,
            amount=amount,
            transaction_time=transaction_time,
            customer_city=customer_city,
            watchlist_reason=watchlist_reason
        )
        
        if success:
            print(f"✅ URGENT fraud alert sent to {customer_email}")
            print(f"   Transaction: {transaction_id} | Card: ****{card_number[-4:]} | Reason: {watchlist_reason}")
        else:
            print(f"❌ FAILED to send alert to {customer_email} for transaction {transaction_id}")
    
    print(f"\n{'='*60}")
    print(f"📊 Batch {batch_id} processing complete")
    print(f"{'='*60}\n")


@dp.append_flow(target="fraud_card_email_sink")
def fraud_card_alert_flow() -> DataFrame:
    """
    Stream fraud transactions from watchlist matches and send urgent email alerts
    """
    return spark.readStream.table("stream_layer.gold.fraud_transactions")