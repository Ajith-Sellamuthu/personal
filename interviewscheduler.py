import os
import shutil
import streamlit as st
import pandas as pd
import uuid
import hashlib
from datetime import datetime, timedelta
from automation_modules import GoogleSheetReader, Gmail
from googleapiclient.discovery import build
from google.oauth2 import service_account as g_service_account

# --- Load credentials from secrets ---
credentials_email = st.secrets["credentials"]["email"]
credentials_password = st.secrets["credentials"]["app_password"]
service_account_info = dict(st.secrets["service_account"])

# --- Setup data directory ---
if not os.path.exists("data"):
    os.makedirs("data")

# --- Google Sheet Setup ---
gsheet_url = "https://docs.google.com/spreadsheets/d/10a42eH_sS82tm1GX3U7u0uvmNjTEVuVAnnYfjbDakOc/edit?gid=0#gid=0"
worksheet_name = "Sheet1"
sheet_reader = GoogleSheetReader(gsheet_url, service_account_info)
df = sheet_reader.get_df_from_sheets(worksheet_name)

# --- Token Generation ---
def generate_token(email):
    salt = "navi_interview_salt"
    return hashlib.sha256((email + salt).encode()).hexdigest()

# --- Step 1: Send scheduling links to applicants ---
def send_scheduling_links():
    base_url = "https://your-app-url.com"  # Replace this
    pending_df = df[df["Status"].str.lower() == "interview to be scheduled"]
    if pending_df.empty:
        st.success("No applicants currently need interview scheduling.")
        return

    gmail_sender = Gmail(credentials_email, credentials_password)
    for idx, row in pending_df.iterrows():
        applicant_email = row["Mail"]
        applicant_name = row.get("Name", applicant_email)
        token = generate_token(applicant_email)
        scheduling_link = f"{base_url}/?token={token}"
        subject = "Schedule Your Interview"
        body = (
            f"Dear {applicant_name},<br><br>"
            "Please use the link below to select your preferred interview slot:<br>"
            f"<a href='{scheduling_link}'>{scheduling_link}</a><br><br>"
            "Best regards,<br>Recruitment Team"
        )
        gmail_sender.send_email(
            sender_name="Recruitment Team",
            receiver_email=applicant_email,
            cc_recipient=None,
            subject=subject,
            body=body,
            is_html=True
        )
    st.success("Scheduling links sent to all applicants needing interview scheduling.")

# --- Step 2: Streamlit app for slot selection ---
def get_available_slots(calendar_service, calendar_id, days_ahead=7):
    now = datetime.utcnow().isoformat() + 'Z'
    future = (datetime.utcnow() + timedelta(days=days_ahead)).isoformat() + 'Z'
    events_result = calendar_service.events().list(
        calendarId=calendar_id,
        timeMin=now,
        timeMax=future,
        singleEvents=True,
        orderBy='startTime'
    ).execute()
    events = events_result.get('items', [])
    slots = []
    for event in events:
        interviewer_email = None
        if 'description' in event and '@' in event['description']:
            interviewer_email = event['description'].strip()
        elif 'summary' in event and '@' in event['summary']:
            interviewer_email = event['summary'].strip()
        else:
            continue
        start = event['start'].get('dateTime', event['start'].get('date'))
        end = event['end'].get('dateTime', event['end'].get('date'))
        slots.append({
            "event_id": event['id'],
            "start": start,
            "end": end,
            "interviewer_email": interviewer_email
        })
    return slots

def schedule_interview(token):
    calendar_id = "ajithrocker07@gmail.com"
    SCOPES = ['https://www.googleapis.com/auth/calendar']
    credentials = g_service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    calendar_service = build('calendar', 'v3', credentials=credentials)

    df = sheet_reader.get_df_from_sheets(worksheet_name)
    df["token"] = df["Mail"].apply(generate_token)
    applicant_row = df[df["token"] == token]
    if applicant_row.empty:
        st.error("Invalid or expired link.")
        shutil.rmtree("data")
        st.stop()
    applicant_row = applicant_row.iloc[0]
    applicant_email = applicant_row["Mail"]
    applicant_name = applicant_row.get("Name", applicant_email)
    status = applicant_row["Status"].lower()
    if status != "interview to be scheduled":
        st.info("Your interview has already been scheduled or is not pending scheduling.")
        shutil.rmtree("data")
        st.stop()

    st.title("Select Your Interview Slot")
    st.write(f"Welcome, {applicant_name}! Please select your preferred interview slot below.")

    slots = get_available_slots(calendar_service, calendar_id)
    if not slots:
        st.warning("No available interview slots found. Please try again later.")
        shutil.rmtree("data")
        st.stop()

    slot_labels = [
        f"{datetime.fromisoformat(slot['start']).strftime('%a, %d %b %Y %I:%M %p')} - {datetime.fromisoformat(slot['end']).strftime('%I:%M %p')} (Interviewer: {slot['interviewer_email']})"
        for slot in slots
    ]
    selected_slot_idx = st.selectbox("Select an available slot:", range(len(slots)), format_func=lambda i: slot_labels[i])
    selected_slot = slots[selected_slot_idx]

    if st.button("Confirm Interview Slot"):
        event = {
            'summary': f"Interview: {applicant_name}",
            'description': f"Interview with {applicant_name} ({applicant_email})",
            'start': {'dateTime': selected_slot['start'], 'timeZone': 'UTC'},
            'end': {'dateTime': selected_slot['end'], 'timeZone': 'UTC'},
            'attendees': [
                {'email': applicant_email},
                {'email': selected_slot['interviewer_email']}
            ],
            'reminders': {
                'useDefault': True,
            }
        }
        calendar_service.events().insert(calendarId=calendar_id, body=event, sendUpdates='all').execute()
        calendar_service.events().delete(calendarId=calendar_id, eventId=selected_slot['event_id']).execute()
        df.loc[df["Mail"] == applicant_email, "Status"] = "interview scheduled"
        df = df.drop(columns=["token"])
        sheet_reader.set_df_in_sheets(worksheet_name, df, include_headers=True)
        st.success(f"Interview scheduled for {applicant_name} on {datetime.fromisoformat(selected_slot['start']).strftime('%a, %d %b %Y %I:%M %p')} with {selected_slot['interviewer_email']}.")
        shutil.rmtree("data")
        st.stop()

# --- Streamlit Routing Logic ---
def main():
    st.sidebar.title("Interview Scheduling Admin")
    admin_action = st.sidebar.selectbox("Admin Actions", ["None", "Send Scheduling Links"])
    if admin_action == "Send Scheduling Links":
        send_scheduling_links()
        shutil.rmtree("data")
        st.stop()

    query_params = st.experimental_get_query_params()
    token = query_params.get("token", [None])[0]
    if token:
        schedule_interview(token)
    else:
        st.info("Please use your unique scheduling link to select an interview slot.")

    if os.path.exists("data"):
        shutil.rmtree("data")

if __name__ == "__main__":
    main()
  
