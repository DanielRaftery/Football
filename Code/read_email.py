import imaplib
from read_data import read_fixtures
import email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time


def connect_email_read(address, password):
    gmail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
    gmail.login(address, password)
    gmail.list()
    gmail.select("inbox")
    return gmail


def connect_email_send(address, password):
    session = smtplib.SMTP('smtp.gmail.com', 587)
    session.starttls()
    session.login(address, password)
    return session


def scan_emails():
    while True:
        mail = connect_email_read(email_address, email_password)
        send_session = connect_email_send(email_address, email_password)
        result, data = mail.uid('search', None, "(UNSEEN)")
        body = []
        subject = ""
        if result == "OK":
            try:
                latest_unread_email = data[0].split()[-1]
            except IndexError:
                latest_unread_email = False

            if latest_unread_email:
                print("\nNew email!")
                fetch_result, fetch_data = mail.uid('fetch', latest_unread_email, '(RFC822)')
                if fetch_result == "OK":
                    raw_email = email.message_from_string(fetch_data[0][1].decode('utf-8'))
                    email_subject = str(raw_email).split("Subject: ", 1)[1].split("\nTo:", 1)[0].split("\n")[0]
                    league, date = email_subject.split(" ")
                    fixture_data = read_fixtures(league, date)
                    subject = "{} {} fixture predictions".format(date, league_dict[league])

                    for i in fixture_data:
                        body.append("\nProbabilities for {} - {}".format(i[0], i[1]) +
                                    "\nHome win: {}%".format(i[2]['home win']) +
                                    "\nDraw: {}%".format(i[2]['draw']) +
                                    "\nAway win: {}%".format(i[2]['away win']) +
                                    "\nOver 2.5 Goals: {}%".format(i[2]['over']))
                    body = "\n".join(body)

        if subject and body:
            message = MIMEMultipart()
            message['From'] = email_address
            message['To'] = valid_sender
            message['Subject'] = subject
            message.attach(MIMEText(body, 'plain'))
            email_text = message.as_string()
            send_session.sendmail(email_address, valid_sender, email_text)
            print("Sent predictions!")
        time.sleep(60)


if __name__ == "__main__":
    league_dict = {"premier_league": "Premier League",
                   "championship": "Championship",
                   "league_one": "League One",
                   "league_two": "League Two"}

    with open("auth.txt", "r") as file:
        text = file.readlines()

    email_password = str(text[8].strip("\n"))
    email_address = str(text[9].strip("\n"))
    valid_sender = str(text[10].strip("\n"))
    scan_emails()
