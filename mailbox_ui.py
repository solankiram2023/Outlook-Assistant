import streamlit as st
from datetime import datetime
from streamlit_quill import st_quill
from components import get_initials, get_category

st.set_page_config(layout="wide")

# Custom CSS
with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Start session state
if 'emails' not in st.session_state:
    st.session_state.emails = [
    {
        "id": 1,
        "sender": "John Doe",
        "email": "john.doe@example.com",
        "subject": "Weekly Team Meeting",
        "content": "Hi Team,\n\nI hope you're all doing well. I wanted to send out a quick reminder about our weekly sync meeting, which is scheduled for tomorrow at 10 AM. As usual, we'll be discussing the progress on ongoing projects, any roadblocks you're facing, and any other relevant updates that need attention.\n\nPlease make sure to prepare any updates or action items you'd like to bring to the table. If there are any topics you'd like to add to the agenda, feel free to let me know before the meeting.\n\nLooking forward to seeing you all tomorrow!\n\nBest regards,\nJohn",
        "date": "2024-03-22 10:30:00",
        "read": False,
        "starred": False,
        "category": "Work",
        "attachments": []
    },
    {
        "id": 2,
        "sender": "Chris Martin",
        "email": "chris.martin@example.com",
        "subject": "Re: Urgent: Client Issue Needs Attention",
        "content": "Hi Team,\n\nI hope you're all having a productive day. Unfortunately, we've encountered a significant issue with one of our clients regarding their most recent order. There are concerns on their end about discrepancies in the details, and we need to assess our files immediately to ensure there are no errors from our side.\n\nCould everyone please check the relevant order files, shipment details, and communications to see if any discrepancies or issues were noted? We need to respond to the client ASAP to maintain our professional standing and avoid further complications.\n\nIf anyone spots anything out of the ordinary, please share your findings with the group so we can address it as a team.\n\nBest,\nChris",
        "date": "2024-03-22 16:45:00",
        "read": False,
        "starred": True,
        "category": "Client Issue",
        "attachments": [
            {
                "name": "order_details.pdf",
                "size": "2.4 MB",
                "type": "application/pdf"
            },
            {
                "name": "shipment_log.xlsx",
                "size": "856 KB",
                "type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            }
        ]
    },
    {
        "id": 3,
        "sender": "Emma Stone",
        "email": "emma.stone@example.com",
        "subject": "Marketing Campaign Proposal",
        "content": "Hello Team,\n\nI wanted to share an exciting new proposal for our upcoming marketing campaign. After analyzing market trends and customer feedback, I've outlined a plan that I believe will resonate well with our target audience.\n\nPlease review the attached proposal document and let me know your thoughts. We can discuss it in our next meeting or via email if you prefer.\n\nLooking forward to your feedback!\n\nBest regards,\nEmma",
        "date": "2024-03-23 09:00:00",
        "read": False,
        "starred": False,
        "category": "Marketing",
        "attachments": [
            {
                "name": "marketing_campaign_proposal.pdf",
                "size": "3.1 MB",
                "type": "application/pdf"
            }
        ]
    },
    {
        "id": 4,
        "sender": "Laura Green",
        "email": "laura.green@example.com",
        "subject": "New Employee Onboarding Checklist",
        "content": "Hi Team,\n\nAs we prepare for the arrival of our new team members, I’ve put together a detailed onboarding checklist to ensure a smooth transition. Please review the attached document and let me know if you think anything should be added or modified.\n\nI want to make sure we provide a welcoming experience, so your input would be greatly appreciated!\n\nThanks,\nLaura",
        "date": "2024-03-23 11:15:00",
        "read": False,
        "starred": False,
        "category": "HR",
        "attachments": [
            {
                "name": "onboarding_checklist.pdf",
                "size": "1.9 MB",
                "type": "application/pdf"
            }
        ]
    },
    {
        "id": 5,
        "sender": "James Lee",
        "email": "james.lee@example.com",
        "subject": "Product Launch Update",
        "content": "Hi Everyone,\n\nI wanted to provide a quick update on the status of the product launch. We are still on track for the scheduled release date, but there are a few final tweaks needed in the marketing materials and the user documentation.\n\nPlease check the attached files and let me know if any further changes are required. Let’s aim to have everything finalized by the end of this week.\n\nThanks!\n\nBest,\nJames",
        "date": "2024-03-23 13:30:00",
        "read": False,
        "starred": True,
        "category": "Product Launch",
        "attachments": [
            {
                "name": "marketing_materials.pdf",
                "size": "2.0 MB",
                "type": "application/pdf"
            },
            {
                "name": "user_guide_v2.docx",
                "size": "1.5 MB",
                "type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            }
        ]
    },
    {
        "id": 6,
        "sender": "Oliver Brown",
        "email": "oliver.brown@example.com",
        "subject": "Quarterly Sales Review",
        "content": "Dear Sales Team,\n\nIt’s time for our quarterly sales review! I’ve attached the data for this quarter’s performance, and I’d like to go over the numbers during our next meeting.\n\nPlease come prepared with any questions or insights you may have. We will also be discussing goals for the next quarter, so it’s important to have your feedback ready.\n\nBest regards,\nOliver",
        "date": "2024-03-24 08:00:00",
        "read": False,
        "starred": False,
        "category": "Sales",
        "attachments": [
            {
                "name": "quarterly_sales_report.xlsx",
                "size": "4.3 MB",
                "type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            }
        ]
    },
    {
        "id": 7,
        "sender": "Sophia White",
        "email": "sophia.white@example.com",
        "subject": "Re: IT System Upgrade",
        "content": "Hi Team,\n\nI wanted to follow up on the recent discussions regarding the IT system upgrade. The upgrade is scheduled for next weekend, and I wanted to remind everyone to backup any important files before Friday.\n\nPlease let me know if you need assistance with backups or have any concerns about the upgrade process.\n\nThanks,\nSophia",
        "date": "2024-03-24 10:30:00",
        "read": False,
        "starred": True,
        "category": "IT",
        "attachments": []
    },
    {
        "id": 8,
        "sender": "Mark Wilson",
        "email": "mark.wilson@example.com",
        "subject": "Client Feedback Request",
        "content": "Hello Team,\n\nWe’ve received some valuable feedback from a key client regarding our recent product offering. I’ve attached the full report for your review.\n\nPlease take a look and let me know if you have any thoughts or suggestions on how we can improve our product or service based on this feedback.\n\nBest regards,\nMark",
        "date": "2024-03-24 12:00:00",
        "read": False,
        "starred": False,
        "category": "Client Feedback",
        "attachments": [
            {
                "name": "client_feedback_report.pdf",
                "size": "3.2 MB",
                "type": "application/pdf"
            }
        ]
    },
    {
        "id": 9,
        "sender": "Isabella Harris",
        "email": "isabella.harris@example.com",
        "subject": "Team Building Event - RSVP",
        "content": "Hi Everyone,\n\nWe are planning a team-building event next month and I’d love for everyone to attend. The event will take place on the 15th of April, and it will include a mix of outdoor activities and collaborative exercises.\n\nPlease RSVP by the end of this week if you plan to attend, so we can finalize logistics.\n\nLooking forward to a fun and productive day!\n\nBest,\nIsabella",
        "date": "2024-03-24 14:15:00",
        "read": False,
        "starred": False,
        "category": "Team Building",
        "attachments": []
    },
    {
        "id": 10,
        "sender": "Benjamin King",
        "email": "benjamin.king@example.com",
        "subject": "Monthly Budget Review",
        "content": "Dear Team,\n\nIt’s time for our monthly budget review. I’ve attached the updated budget spreadsheet for your review. Please take a moment to go over the figures and bring any questions or concerns to our next meeting.\n\nWe need to ensure we stay on track with our financial goals, so your input will be critical.\n\nBest regards,\nBenjamin",
        "date": "2024-03-25 08:45:00",
        "read": False,
        "starred": False,
        "category": "Finance",
        "attachments": [
            {
                "name": "budget_review_march.xlsx",
                "size": "2.8 MB",
                "type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            }
        ]
    },
    {
        "id": 11,
        "sender": "Rachel Adams",
        "email": "rachel.adams@example.com",
        "subject": "Re: Website Maintenance",
        "content": "Hi Team,\n\nJust a quick reminder about the scheduled website maintenance tomorrow night. The maintenance window will be from 11 PM to 3 AM, during which time the website will be temporarily unavailable.\n\nPlease notify your clients or customers if they may be affected. If you have any questions or need assistance, feel free to reach out.\n\nBest,\nRachel",
        "date": "2024-03-25 10:00:00",
        "read": False,
        "starred": True,
        "category": "Website",
        "attachments": []
    },
    {
        "id": 12,
        "sender": "Lucas Carter",
        "email": "lucas.carter@example.com",
        "subject": "Follow-Up on Proposal Submission",
        "content": "Dear Team,\n\nI just wanted to follow up on the proposal we submitted to the client last week. Have there been any updates or feedback from their side? We need to stay proactive and ensure we address any questions or concerns they might have.\n\nPlease provide any updates you have on the status of the proposal.\n\nBest regards,\nLucas",
        "date": "2024-03-25 15:30:00",
        "read": False,
        "starred": False,
        "category": "Proposal",
        "attachments": []
    }

]

if 'selected_email_id' not in st.session_state:
    st.session_state.selected_email_id = None

with st.sidebar:
    st.button("Compose", type="primary", use_container_width=True)
    st.markdown("---")
    
    nav_options = {
        "Inbox"     : "inbox",
        "Starred"   : "starred",
        "Snoozed"   : "snoozed",
        "Sent"      : "sent",
        "Drafts"    : "drafts",
        "Trash"     : "trash"
    }
    
    selected_nav = None
    for label, value in nav_options.items():
        if st.button(label, key=f"nav_{value}", use_container_width=True):
            selected_nav = value

col1, col2 = st.columns([1, 2])

# Email list column
with col1:
    st.markdown("### Inbox")
    
    search_query = st.text_input(
        label               = "Search",
        label_visibility    = "hidden",
        key                 = "search",
        placeholder         = "Search mail..."
    )
    
    email_list_container = st.container()
    
    with email_list_container:
        for email in st.session_state.emails:
            category, bg_color = get_category(email)
            initials = get_initials(email['sender'])
            
            with st.container():
                cols = st.columns([6, 1])
                
                # Raw HTML because Streamlit
                with cols[0]:
                    
                    email_html = f"""
                    <div class="email-list-item" onclick="handleClick({email['id']})">
                        <div class="profile-pic" style="background-color: {bg_color}">
                            <span style="color: gray">{initials}</span>
                        </div>
                        <div class="email-content-preview">
                            <div class="sender-name">{email['sender']}</div>
                            <div class="email-subject">{email['subject']}</div>
                            <div>{email['content'].replace('\n', ' ')[:50] + "..."}</div>
                            <div class="category-tag" style="background-color: {bg_color}">
                                {category}
                            </div>
                        </div>
                    """

                    if email['starred']:
                        email_html += f"""<div class="flag-icon">🚩</div>"""
                    
                    email_html += """</div>"""
                    st.markdown(email_html, unsafe_allow_html=True)
                
                with cols[1]:
                    if st.button("View", key=f"select_{email['id']}"):
                        st.session_state.selected_email_id = email['id']
                        email['read'] = True
                        st.rerun()

# Email content column
with col2:

    selected_email = next(
        (email for email in st.session_state.emails 
         if email['id'] == st.session_state.selected_email_id),
        None
    )
    
    if selected_email:
        header_col1, header_col2 = st.columns([10, 1])
        
        with header_col1:
            st.markdown(f"### {selected_email['subject']}")
        
        with header_col2:
            menu_container = st.container()
            
            # Overflow menu
            with menu_container:
                if st.button("⋮", key="menu_button"):
                    st.session_state.show_menu = not st.session_state.get('show_menu', False)
                
                if st.session_state.get('show_menu', False):
                    st.markdown("""
                        <div class="dropdown-container">
                            <div class="dropdown-item">Archive</div>
                            <div class="dropdown-item">Delete</div>
                            <div class="dropdown-item">Mark as unread</div>
                            <div class="dropdown-item">Snooze</div>
                        </div>
                    """, unsafe_allow_html=True)
        
        st.markdown(f"**From:** {selected_email['sender']} <{selected_email['email']}>")
        st.markdown(f"**Date:** {datetime.strptime(selected_email['date'], '%Y-%m-%d %H:%M:%S').strftime('%b %d, %Y %I:%M %p')}")
        
        st.markdown("---")
        st.markdown(selected_email['content'])
        
        st.markdown("---")

        # Attachments section
        if selected_email.get('attachments'):
            st.markdown("**Attachments:**")
            
            for attachment in selected_email['attachments']:
                col1, col2 = st.columns([1, 6])
                
                with col1:
                    st.markdown(f"{attachment['name']}")
                
                with col2:
                    st.markdown(f"({attachment['size']})")

        st.markdown("---")
        
        # Reply section
        st.markdown("#### Reply")
        content = st_quill(placeholder="Reply to this email...")
        col_reply1, col_reply2 = st.columns([1, 13])
        
        with col_reply1:
            st.button("Send", type="primary", use_container_width=False)
        
        with col_reply2:
            st.button("Save as Draft", use_container_width=False)
    else:
        st.markdown("### Select an email to read")

# Footer
st.markdown("---")
st.markdown(
    f"""
    <div class="refresh-time">
        Last refreshed at {datetime.now().strftime('%I:%M %p')}
    </div>
    """,
    unsafe_allow_html=True
)