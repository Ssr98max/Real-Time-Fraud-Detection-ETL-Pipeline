import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

# Fetch Gmail credentials from secrets (outside the handler for serialization compatibility)
gmail_sender = "sutradharj61@gmail.com" 
gmail_password = dbutils.secrets.get(scope="secret-scope", key="gmail_api_key")


def send_fraud_alert_email(to_email, customer_name, transaction_id, amount, currency, country, card_number):
    """
    Send a fraud alert email to the customer
    """
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = gmail_sender
        msg['To'] = to_email
        msg['Subject'] = f'⚠️ Fraud Alert: High-Value Transaction Detected'

        # Email body
        body = f"""
        Dear {customer_name},

        We have detected a high-value transaction on your account that may require your attention:

        Transaction Details:
        -------------------
        Transaction ID: {transaction_id}
        Amount: {amount} {currency}
        Card Number: ****{card_number[-4:]}
        Country: {country}
        
        If you did not authorize this transaction, please contact us immediately.
        
        If this transaction was authorized by you, no action is needed.

        Best regards,
        Fraud Detection Team
        """

        msg.attach(MIMEText(body, 'plain'))

        # Connect to Gmail SMTP server and send email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(gmail_sender, gmail_password)
            server.send_message(msg)
        
        return True
    except Exception as e:
        print(f"Failed to send email to {to_email}: {str(e)}")
        return False


@dp.foreach_batch_sink(name="fraud_email_sink")
def fraud_email_sink(df: DataFrame, batch_id: int):
    """
    ForEachBatch sink handler that sends email alerts for fraud transactions
    """
    # Collect the fraud transactions in this micro-batch
    fraud_transactions = df.collect()
    
    # Send email for each fraud transaction
    for row in fraud_transactions:
        customer_email = row.customer_email
        customer_name = row.customer_name
        transaction_id = row.transaction_id
        amount = row.high_amount
        currency = row.currency
        country = row.country
        card_number = row.customer_card_no
        
        # Send the email
        success = send_fraud_alert_email(
            to_email=customer_email,
            customer_name=customer_name,
            transaction_id=transaction_id,
            amount=amount,
            currency=currency,
            country=country,
            card_number=card_number
        )
        
        if success:
            print(f"✅ Fraud alert email sent to {customer_email} for transaction {transaction_id}")
        else:
            print(f"❌ Failed to send email to {customer_email} for transaction {transaction_id}")


@dp.append_flow(target="fraud_email_sink")
def fraud_alert_flow() -> DataFrame:
    """
    Stream fraud transactions from the gold layer and send email alerts
    """
    return spark.readStream.table("stream_layer.gold.likely_fraud_transactions")