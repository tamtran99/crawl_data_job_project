import psycopg2
import telebot
import configparser

# Database Configuration
config = configparser.ConfigParser()
config.read('config.ini')

DB_CONFIG = config['Database']

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = config['TELEGRAM']['BOT_TOKEN']
TELEGRAM_CHANNEL_ID = config['TELEGRAM']['CHANNEL_ID']

# Initialize Telegram Bot
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

def fetch_and_send_messages():
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # SQL Query to get recent job posts
        query = """
        SELECT JOB_ID, TIME_POSTED, COMPANY_NAME, JOB_TITLE, URL_JOB
        FROM crawl_linkedin.linkedin_data_dwh
        WHERE PROCESS_DATE >= CURRENT_DATE - INTERVAL '1 day'
        AND (TIME_POSTED LIKE '%day%' OR TIME_POSTED LIKE '%hour%');
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        sent_count = 0  # ✅ Initialize counter before loop

        for row in rows:
            job_id, time_posted, company_name, job_title, url_job = row  # Extract values
            
            # Check if JOB_ID has already been sent
            cursor.execute("SELECT 1 FROM crawl_linkedin.sent_jobs_telegram WHERE job_id = %s::VARCHAR", (str(job_id),))
            if cursor.fetchone():
                continue  # Skip if already sent

            # Format message using Markdown
            message = f"""
📢 *New Job Alert!*
🏢 Company: *{company_name}*
📌 Position: *{job_title}*
🕒 Posted: `{time_posted}`
🔗 [Apply Here]({url_job})
            """

            # Send formatted message to Telegram
            bot.send_message(TELEGRAM_CHANNEL_ID, message, parse_mode="Markdown")
            print(f"✅ Sent: {job_id} - {job_title} at {company_name}")

            # ✅ Only insert if a job was actually sent
            cursor.execute("INSERT INTO crawl_linkedin.sent_jobs_telegram (job_id) VALUES (%s)", (job_id,))
            conn.commit()

            sent_count += 1  # Increment counter

        # ✅ Print message if no new jobs were sent (but do not insert anything)
        if sent_count == 0:
            print("❌ No new jobs to send.")

        # Close database connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_and_send_messages()