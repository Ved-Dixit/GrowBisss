import streamlit as st
import psycopg2
from psycopg2 import sql
import os
from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import hashlib
import jwt
import time
import docx
from docx.shared import Inches
from io import BytesIO
import pytz
from gtts import gTTS
import speech_recognition as sr
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from PIL import Image
import json
from fpdf import FPDF
from datetime import datetime, timedelta, timezone, date
# Load environment variables

# Database Connection
def get_db_connection():
    conn = psycopg2.connect(
        dbname = "railway",
        user = "postgres",
        password = "GYEaWyjVgFkHqPcELGqBsPzOAfwgtqdR",
        host = "hopper.proxy.rlwy.net",
        port = "47078"
    )
    return conn

# Initialize database tables
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create tables if they don't exist
    tables = [
        """
        CREATE TABLE IF NOT EXISTS businesses (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            subscription_type VARCHAR(50) DEFAULT 'freemium',
            subscription_expiry DATE DEFAULT (CURRENT_DATE + INTERVAL '3 months')
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            sender_type VARCHAR(30) NOT NULL, -- 'business', 'investor', 'service_provider'
            sender_id INTEGER NOT NULL,
            receiver_type VARCHAR(30) NOT NULL,
            receiver_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            read_at TIMESTAMP NULL -- Corrected line: Removed NULLABLE, NULL is implied or can be explicit
            -- Index for faster conversation lookup (optional, but good for performance)
            -- CREATE INDEX IF NOT EXISTS idx_messages_conversation_pair1 ON messages (sender_type, sender_id, receiver_type, receiver_id, created_at);
            -- CREATE INDEX IF NOT EXISTS idx_messages_conversation_pair2 ON messages (receiver_type, receiver_id, sender_type, sender_id, created_at);
        )
        """,

        """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            price DECIMAL(10,2) NOT NULL,
            quantity INTEGER NOT NULL,
            category VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) NOT NULL,
            position VARCHAR(100),
            department VARCHAR(100),
            salary DECIMAL(12,2),
            join_date DATE,
            last_appraisal_date DATE,
            performance_score INTEGER,
            skills TEXT[],
            UNIQUE(business_id, email)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS projects (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            client VARCHAR(100),
            start_date DATE,
            end_date DATE,
            budget DECIMAL(12,2),
            status VARCHAR(50),
            manager_id INTEGER REFERENCES employees(id),
            progress INTEGER DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS documents (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            title VARCHAR(200) NOT NULL,
            content TEXT,
            doc_type VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER REFERENCES employees(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS market_data (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            industry VARCHAR(100),
            metric VARCHAR(100),
            value DECIMAL(12,2),
            date DATE,
            source VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS project_assignments (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
            employee_id INTEGER REFERENCES employees(id) ON DELETE CASCADE,
            assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(business_id, project_id, employee_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS investors (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            name VARCHAR(100) NOT NULL,
            firm VARCHAR(100),
            email VARCHAR(100),
            investment_focus VARCHAR(200),
            portfolio_companies TEXT[],
            last_contact DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS schemes (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            eligibility TEXT,
            benefits TEXT,
            deadline DATE,
            sector VARCHAR(100),
            is_govt BOOLEAN
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS opportunities (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            title VARCHAR(200) NOT NULL,
            description TEXT,
            category VARCHAR(100),
            deadline DATE,
            reward TEXT,
            link VARCHAR(200)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tax_records (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            financial_year VARCHAR(20),
            total_income DECIMAL(12,2),
            tax_paid DECIMAL(12,2),
            filing_date DATE,
            status VARCHAR(50),
            notes TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ipo_data (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            company_name VARCHAR(100),
            issue_size DECIMAL(12,2),
            price_range VARCHAR(50),
            open_date DATE,
            close_date DATE,
            status VARCHAR(50),
            allotment_date DATE,
            listing_date DATE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS service_providers (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            name VARCHAR(100),
            service_type VARCHAR(100),
            contact_email VARCHAR(100),
            rating DECIMAL(3,1),
            experience_years INTEGER,
            pricing TEXT,
            availability BOOLEAN
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS attendance (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            employee_id INTEGER REFERENCES employees(id),
            date DATE,
            status VARCHAR(20),
            check_in TIME,
            check_out TIME
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS job_openings (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            title VARCHAR(100),
            department VARCHAR(100),
            description TEXT,
            requirements TEXT[],
            experience_needed VARCHAR(50),
            posted_date DATE,
            status VARCHAR(20)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS invoices (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            invoice_number VARCHAR(50) UNIQUE,
            customer_name VARCHAR(100),
            customer_email VARCHAR(100),
            issue_date DATE,
            due_date DATE,
            total_amount DECIMAL(12,2),
            tax_amount DECIMAL(12,2),
            status VARCHAR(20) DEFAULT 'pending',
            items JSONB
        )
        """,
         """
        CREATE TABLE IF NOT EXISTS investors (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE NULL, 
            name VARCHAR(100) NOT NULL,
            firm VARCHAR(100),
            email VARCHAR(100) UNIQUE NOT NULL, 
            password_hash VARCHAR(255),         
            investment_focus VARCHAR(200),
            portfolio_companies TEXT[],
            last_contact DATE,
            profile_description TEXT,          
            website_url VARCHAR(255),         
            linkedin_profile VARCHAR(255),     
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
        )
        """,
        # ...
        """
        CREATE TABLE IF NOT EXISTS service_providers (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE NULL, 
            name VARCHAR(100) NOT NULL,
            service_type VARCHAR(100) NOT NULL, 
            email VARCHAR(100) UNIQUE NOT NULL, 
            password_hash VARCHAR(255),    
            contact_email VARCHAR(100),   
            rating DECIMAL(3,1),
            experience_years INTEGER,
            pricing TEXT,
            availability BOOLEAN,
            profile_description TEXT,       
            specializations TEXT[],       
            office_address TEXT,          
            website_url VARCHAR(255),     
            linkedin_profile VARCHAR(255),   
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_sessions (
            id SERIAL PRIMARY KEY,
            business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
            session_token VARCHAR(255) UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP
        )
        """
    ]
    
    for table in tables:
        try:
            cur.execute(table)
        except Exception as e:
            st.error(f"Error creating table: {e}")
    
    conn.commit()
    cur.close()
    conn.close()

# Security functions
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(plain_password, hashed_password):
    return hash_password(plain_password) == hashed_password

def generate_jwt(business_id):
    payload = {
        'business_id': business_id,
        'exp': datetime.utcnow() + timedelta(hours=24)
    }
    return jwt.encode(payload, os.getenv('JWT_SECRET', 'secret_key'), algorithm='HS256')

def verify_jwt(token):
    try:
        payload = jwt.decode(token, os.getenv('JWT_SECRET', 'secret_key'), algorithms=['HS256'])
        return payload['business_id']
    except:
        return None

# Initialize AI models
class AIModels:
    def __init__(self):
        st.write("AIModels class instance created. Models will be loaded on first use.")
        self._chatbot_tokenizer_instance = None
        self._chatbot_model_instance = None
        self._text_generator_instance = None
        self._sentiment_analyzer_instance = None
        self._translator_en_hi_instance = None
        self._translator_hi_en_instance = None

    @property
    def chatbot_tokenizer(self):
        if self._chatbot_tokenizer_instance is None:
            st.write("Loading chatbot_tokenizer (facebook/blenderbot-400M-distill)...")
            self._chatbot_tokenizer_instance = AutoTokenizer.from_pretrained("facebook/blenderbot-400M-distill")
            st.write("Chatbot_tokenizer loaded.")
        return self._chatbot_tokenizer_instance

    @property
    def chatbot_model(self):
        if self._chatbot_model_instance is None:
            st.write("Loading chatbot_model (facebook/blenderbot-400M-distill)...")
            self._chatbot_model_instance = AutoModelForSeq2SeqLM.from_pretrained("facebook/blenderbot-400M-distill")
            st.write("Chatbot_model loaded.")
        return self._chatbot_model_instance

    @property
    def text_generator(self):
        if self._text_generator_instance is None:
            st.write("Loading text_generator (gpt2)...")
            self._text_generator_instance = pipeline("text-generation", model="gpt2")
            st.write("Text_generator (gpt2) loaded.")
        return self._text_generator_instance

    @property
    def sentiment_analyzer(self):
        if self._sentiment_analyzer_instance is None:
            st.write("Loading sentiment_analyzer (distilbert-base-uncased-finetuned-sst-2-english)...")
            self._sentiment_analyzer_instance = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
            st.write("Sentiment_analyzer loaded.")
        return self._sentiment_analyzer_instance

    @property
    def translator_en_hi(self):
        if self._translator_en_hi_instance is None:
            st.write("Loading translator_en_hi (Helsinki-NLP/opus-mt-en-hi)...")
            self._translator_en_hi_instance = pipeline("translation", model="Helsinki-NLP/opus-mt-en-hi")
            st.write("Translator_en_hi loaded.")
        return self._translator_en_hi_instance

    @property
    def translator_hi_en(self):
        if self._translator_hi_en_instance is None:
            st.write("Loading translator_hi_en (Helsinki-NLP/opus-mt-hi-en)...")
            self._translator_hi_en_instance = pipeline("translation", model="Helsinki-NLP/opus-mt-hi-en")
            st.write("Translator_hi_en loaded.")
        return self._translator_hi_en_instance

    def generate_response(self, prompt):
        # Access models via properties, which will trigger loading on first call
        inputs = self.chatbot_tokenizer([prompt], return_tensors="pt")
        reply_ids = self.chatbot_model.generate(**inputs)
        return self.chatbot_tokenizer.batch_decode(reply_ids, skip_special_tokens=True)[0]

    def generate_text(self, prompt, max_length=150):
        # Access model via property
        return self.text_generator(prompt, max_length=max_length, num_return_sequences=1)[0]['generated_text']

    def analyze_sentiment(self, text):
        # Access model via property
        return self.sentiment_analyzer(text)

    def translate(self, text, target_lang):
        # Access models via properties
        if target_lang == "Hindi":
            return self.translator_en_hi(text)[0]['translation_text']
        elif target_lang == "English":
            return self.translator_hi_en(text)[0]['translation_text']
        return text

# The load_ai_models function remains the same, using @st.cache_resource
@st.cache_resource
def load_ai_models():
    models = AIModels()
    return models

def investor_registration_page():
    st.subheader("Investor Registration")
    with st.form("investor_register_form"):
        name = st.text_input("Full Name / Firm Name")
        email = st.text_input("Login Email")
        password = st.text_input("Password", type="password")
        confirm_password = st.text_input("Confirm Password", type="password")
        firm = st.text_input("Firm (if applicable)")
        investment_focus = st.text_area("Investment Focus (e.g., SaaS, Fintech, Early Stage)")
        profile_description = st.text_area("Brief Profile / About Us")
        website_url = st.text_input("Website URL (Optional)")
        linkedin_profile = st.text_input("LinkedIn Profile URL (Optional)")

        submitted = st.form_submit_button("Register as Investor")
        if submitted:
            if password != confirm_password:
                st.error("Passwords do not match.")
            elif not name or not email or not password:
                st.error("Name, Email, and Password are required.")
            else:
                conn = get_db_connection()
                cur = conn.cursor()
                try:
                    hashed_pw = hash_password(password)
                    cur.execute(
                        """INSERT INTO investors (name, email, password_hash, firm, investment_focus, profile_description, website_url, linkedin_profile)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                        (name, email, hashed_pw, firm, investment_focus, profile_description, website_url, linkedin_profile)
                    )
                    investor_id = cur.fetchone()[0]
                    conn.commit()
                    st.success("Investor registration successful! You can now login.")
                    st.session_state.user_type_for_login = 'investor' # Hint for login page
                    st.session_state.registered_email = email # Pre-fill login
                    # Ideally, redirect to login or auto-login. For now, message.
                except psycopg2.IntegrityError:
                    st.error("This email is already registered as an investor.")
                except Exception as e:
                    st.error(f"An error occurred: {e}")
                finally:
                    cur.close()
                    conn.close()

def service_provider_registration_page():
    st.subheader("Service Provider Registration (CA, Legal, etc.)")
    with st.form("service_provider_register_form"):
        name = st.text_input("Full Name / Firm Name")
        email = st.text_input("Login Email")
        password = st.text_input("Password", type="password")
        confirm_password = st.text_input("Confirm Password", type="password")
        
        service_type_options = ["legal", "ca", "insurance", "consulting", "other"] # Add more as needed
        service_type = st.selectbox("Primary Service Type", service_type_options)
        
        contact_email_public = st.text_input("Public Contact Email (Optional, if different from login)")
        profile_description = st.text_area("Profile / Service Description")
        specializations_str = st.text_input("Specializations (comma-separated, e.g., Corporate Law, Tax Audit)")
        experience_years = st.number_input("Years of Experience", min_value=0, step=1)
        office_address = st.text_area("Office Address (Optional)")
        website_url = st.text_input("Website URL (Optional)")
        linkedin_profile = st.text_input("LinkedIn Profile URL (Optional)")

        submitted = st.form_submit_button("Register as Service Provider")
        if submitted:
            if password != confirm_password:
                st.error("Passwords do not match.")
            elif not name or not email or not password or not service_type:
                st.error("Name, Email, Password, and Service Type are required.")
            else:
                conn = get_db_connection()
                cur = conn.cursor()
                try:
                    hashed_pw = hash_password(password)
                    specializations_list = [s.strip() for s in specializations_str.split(',') if s.strip()]
                    cur.execute(
                        """INSERT INTO service_providers 
                           (name, email, password_hash, service_type, contact_email, profile_description, specializations, experience_years, office_address, website_url, linkedin_profile)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                        (name, email, hashed_pw, service_type, contact_email_public or email, profile_description, specializations_list, experience_years, office_address, website_url, linkedin_profile)
                    )
                    provider_id = cur.fetchone()[0]
                    conn.commit()
                    st.success("Service Provider registration successful! You can now login.")
                    st.session_state.user_type_for_login = 'service_provider'
                    st.session_state.registered_email = email
                except psycopg2.IntegrityError:
                    st.error("This email is already registered as a service provider.")
                except Exception as e:
                    st.error(f"An error occurred: {e}")
                finally:
                    cur.close()
                    conn.close()

def investor_portal(investor_id, ai_models):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT name, email, firm, investment_focus, profile_description, website_url, linkedin_profile FROM investors WHERE id = %s", (investor_id,))
    investor_data = cur.fetchone()
    cur.close()
    conn.close()

    if not investor_data:
        st.error("Investor profile not found.")
        return

    st.title(f"🚀 Investor Portal: {investor_data[0]}")
    
    tab1, tab2 = st.tabs(["My Profile", "Messages"])

    with tab1:
        st.subheader("My Profile")
        st.write(f"**Name/Firm:** {investor_data[0]}")
        st.write(f"**Login Email:** {investor_data[1]}")
        st.write(f"**Registered Firm:** {investor_data[2] or 'N/A'}")
        st.write(f"**Investment Focus:** {investor_data[3] or 'N/A'}")
        st.write(f"**Profile Description:** {investor_data[4] or 'N/A'}")
        st.write(f"**Website:** {investor_data[5] or 'N/A'}")
        st.write(f"**LinkedIn:** {investor_data[6] or 'N/A'}")
        # Add profile edit form here later

    with tab2:
        st.subheader("Messages from Businesses")
        # Simplified chat view for investor (can be expanded like business chat_module)
        # For now, just show incoming messages. Replies would need the full chat UI.
        # This requires the investor to be the "receiver" in the messages table.
        chat_module_for_entity(entity_id=investor_id, entity_type='investor', entity_name=investor_data[0], ai_models=ai_models)


def service_provider_portal(provider_id, ai_models):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT name, email, service_type, contact_email, profile_description, specializations, experience_years, office_address, website_url, linkedin_profile FROM service_providers WHERE id = %s", (provider_id,))
    provider_data = cur.fetchone()
    cur.close()
    conn.close()

    if not provider_data:
        st.error("Service Provider profile not found.")
        return

    st.title(f"🚀 {provider_data[2].capitalize()} Portal: {provider_data[0]}")

    tab1, tab2 = st.tabs(["My Profile", "Messages"])
    with tab1:
        st.subheader("My Profile")
        st.write(f"**Name/Firm:** {provider_data[0]}")
        st.write(f"**Login Email:** {provider_data[1]}")
        st.write(f"**Service Type:** {provider_data[2].capitalize()}")
        st.write(f"**Public Contact Email:** {provider_data[3] or 'N/A'}")
        st.write(f"**Profile Description:** {provider_data[4] or 'N/A'}")
        st.write(f"**Specializations:** {', '.join(provider_data[5]) if provider_data[5] else 'N/A'}")
        st.write(f"**Experience:** {provider_data[6] or 'N/A'} years")
        st.write(f"**Office Address:** {provider_data[7] or 'N/A'}")
        st.write(f"**Website:** {provider_data[8] or 'N/A'}")
        st.write(f"**LinkedIn:** {provider_data[9] or 'N/A'}")
        # Add profile edit form here later
        
    with tab2:
        st.subheader("Messages from Businesses")
        chat_module_for_entity(entity_id=provider_id, entity_type='service_provider', entity_name=provider_data[0], ai_models=ai_models)

# Generic Chat Module for Non-Business Entities
def chat_module_for_entity(entity_id, entity_type, entity_name, ai_models):
    st.header(f"💬 Messaging for {entity_name}")

    conn = get_db_connection()
    cur = conn.cursor()

    # State variables
    if f'chat_partner_type_{entity_type}_{entity_id}' not in st.session_state:
        st.session_state[f'chat_partner_type_{entity_type}_{entity_id}'] = None # Will always be 'business' for them
    if f'chat_partner_id_{entity_type}_{entity_id}' not in st.session_state:
        st.session_state[f'chat_partner_id_{entity_type}_{entity_id}'] = None
    if f'chat_partner_name_{entity_type}_{entity_id}' not in st.session_state:
        st.session_state[f'chat_partner_name_{entity_type}_{entity_id}'] = None

    # --- Sidebar: List businesses they have chatted with ---
    st.sidebar.subheader("Conversations")
    try:
        # Businesses that have sent a message to this entity OR this entity has sent a message to
        query_partners = """
        SELECT DISTINCT sender_id, sender_type FROM messages
        WHERE receiver_id = %(current_id)s AND receiver_type = %(current_type)s AND sender_type = 'business'
        UNION
        SELECT DISTINCT receiver_id, receiver_type FROM messages
        WHERE sender_id = %(current_id)s AND sender_type = %(current_type)s AND receiver_type = 'business';
        """
        cur.execute(query_partners, {'current_id': entity_id, 'current_type': entity_type})
        partners_db = cur.fetchall()
        
        partner_options = {} # Display Name: (db_type, id)
        for p_id, p_type_db in partners_db: # p_type_db will be 'business'
            if p_type_db == 'business':
                cur.execute("SELECT name FROM businesses WHERE id = %s", (p_id,))
                name_result = cur.fetchone()
                if name_result:
                    partner_display_name = f"{name_result[0]} (Business)"
                    partner_options[partner_display_name] = ('business', p_id)
        
        selected_partner_display = st.sidebar.selectbox(
            "Select Business to Chat With:",
            options=["-- Select --"] + list(partner_options.keys()),
            key=f"select_chat_partner_for_{entity_type}_{entity_id}"
        )

        if selected_partner_display != "-- Select --":
            partner_db_type, partner_id_val = partner_options[selected_partner_display]
            st.session_state[f'chat_partner_type_{entity_type}_{entity_id}'] = partner_db_type
            st.session_state[f'chat_partner_id_{entity_type}_{entity_id}'] = partner_id_val
            st.session_state[f'chat_partner_name_{entity_type}_{entity_id}'] = selected_partner_display.split(" (")[0]
            st.rerun()

    except Exception as e_partner_list:
        st.sidebar.error(f"Error loading chat partners: {e_partner_list}")

    # --- Main Chat Area ---
    partner_selected_id = st.session_state.get(f'chat_partner_id_{entity_type}_{entity_id}')
    partner_selected_name = st.session_state.get(f'chat_partner_name_{entity_type}_{entity_id}')

    if partner_selected_id and partner_selected_name:
        st.subheader(f"Chat with {partner_selected_name} (Business)")
        
        # Mark messages as read (messages sent BY the business partner TO this entity)
        try:
            update_read_query = """
                UPDATE messages SET read_at = CURRENT_TIMESTAMP
                WHERE receiver_type = %s AND receiver_id = %s
                  AND sender_type = 'business' AND sender_id = %s
                  AND read_at IS NULL;
            """
            cur.execute(update_read_query, (entity_type, entity_id, partner_selected_id))
            conn.commit()
        except Exception as e_read_update:
            st.warning(f"Could not update read status: {e_read_update}")

        # Fetch messages
        try:
            query_messages = """
                SELECT sender_type, sender_id, content, created_at, read_at
                FROM messages
                WHERE
                    (sender_type = %(current_entity_type)s AND sender_id = %(current_entity_id)s AND receiver_type = 'business' AND receiver_id = %(partner_business_id)s)
                OR
                    (sender_type = 'business' AND sender_id = %(partner_business_id)s AND receiver_type = %(current_entity_type)s AND receiver_id = %(current_entity_id)s)
                ORDER BY created_at ASC;
            """
            cur.execute(query_messages, {
                'current_entity_type': entity_type, 'current_entity_id': entity_id,
                'partner_business_id': partner_selected_id
            })
            messages = cur.fetchall()
            
            chat_container = st.container()
            with chat_container:
                if not messages: st.info("No messages yet. Start the conversation!")
                for msg_sender_type, msg_sender_id, msg_content, msg_created_at, msg_read_at in messages:
                    timestamp_str = msg_created_at.strftime('%Y-%m-%d %H:%M')
                    if msg_sender_type == entity_type and msg_sender_id == entity_id: # Message sent by this entity
                        read_indicator = " (Read)" if msg_read_at else ""
                        st.markdown(f"<div style='text-align: right; margin-left: 20%; margin-bottom: 5px; padding: 10px; background-color: #DCF8C6; border-radius: 10px;'><b>You ({entity_name})</b> ({timestamp_str}){read_indicator}:<br>{msg_content}</div>", unsafe_allow_html=True)
                    else: # Message received from business
                        st.markdown(f"<div style='text-align: left; margin-right: 20%; margin-bottom: 5px; padding: 10px; background-color: #FFFFFF; border-radius: 10px; border: 1px solid #E0E0E0;'><b>{partner_selected_name}</b> ({timestamp_str}):<br>{msg_content}</div>", unsafe_allow_html=True)
            if messages:
                 st.markdown(f"<script>window.scrollTo(0,document.body.scrollHeight);</script>", unsafe_allow_html=True)

        except Exception as e_fetch:
            st.error(f"Error fetching messages: {e_fetch}")

        # Message input
        with st.form(f"new_message_form_{entity_type}_{entity_id}", clear_on_submit=True):
            new_message = st.text_area("Your reply:", key=f"new_message_content_{entity_type}_{entity_id}_{partner_selected_id}")
            send_reply_btn = st.form_submit_button("Send Reply")
            if send_reply_btn and new_message.strip():
                try:
                    cur.execute(
                        "INSERT INTO messages (sender_type, sender_id, receiver_type, receiver_id, content) VALUES (%s, %s, %s, %s, %s)",
                        (entity_type, entity_id, 'business', partner_selected_id, new_message.strip())
                    )
                    conn.commit()
                    st.rerun()
                except Exception as e_send:
                    st.error(f"Error sending reply: {e_send}")
            elif send_reply_btn and not new_message.strip():
                st.warning("Message cannot be empty.")
    else:
        st.info("Select a business from the sidebar to view or continue a conversation.")
    
    cur.close()
    conn.close()

# Authentication functions
def login_page():
    st.title("GrowBis Login")

    # User type selection
    login_user_type = st.radio(
        "Login as:",
        ('Business', 'Investor', 'Service Provider'),
        key='login_user_type_selection',
        horizontal=True,
        index=0 if 'user_type_for_login' not in st.session_state else ['Business', 'Investor', 'Service Provider'].index(st.session_state.user_type_for_login.capitalize()) if st.session_state.user_type_for_login else 0
    )

    default_email = st.session_state.get('registered_email', "")

    if login_user_type == 'Business':
        st.subheader("Business Login")
        with st.form("business_login_form"):
            email = st.text_input("Email", value=default_email if st.session_state.get('user_type_for_login') == 'business' else "")
            password = st.text_input("Password", type="password")
            
            if st.form_submit_button("Login as Business"):
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT id, password_hash, name FROM businesses WHERE email = %s", (email,))
                result = cur.fetchone()
                cur.close()
                conn.close()
                
                if result and verify_password(password, result[1]):
                    st.session_state.token = generate_jwt(result[0]) # Using business_id for JWT for now
                    st.session_state.business_id = result[0]
                    st.session_state.user_type = 'business'
                    st.session_state.user_name = result[2]
                    st.session_state.logged_in_entity_id = result[0]
                    if 'registered_email' in st.session_state: del st.session_state.registered_email
                    if 'user_type_for_login' in st.session_state: del st.session_state.user_type_for_login
                    st.rerun()
                else:
                    st.error("Invalid email or password for Business.")
    
    elif login_user_type == 'Investor':
        st.subheader("Investor Login")
        with st.form("investor_login_form"):
            email = st.text_input("Email", value=default_email if st.session_state.get('user_type_for_login') == 'investor' else "")
            password = st.text_input("Password", type="password")

            if st.form_submit_button("Login as Investor"):
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT id, password_hash, name FROM investors WHERE email = %s", (email,))
                result = cur.fetchone()
                cur.close()
                conn.close()

                if result and verify_password(password, result[1]):
                    st.session_state.user_type = 'investor'
                    st.session_state.logged_in_entity_id = result[0] # This is investor_id
                    st.session_state.user_name = result[2]
                    # No JWT token for investors yet, direct session state login
                    if 'registered_email' in st.session_state: del st.session_state.registered_email
                    if 'user_type_for_login' in st.session_state: del st.session_state.user_type_for_login
                    st.rerun()
                else:
                    st.error("Invalid email or password for Investor.")

    elif login_user_type == 'Service Provider':
        st.subheader("Service Provider Login")
        with st.form("service_provider_login_form"):
            email = st.text_input("Email", value=default_email if st.session_state.get('user_type_for_login') == 'service_provider' else "")
            password = st.text_input("Password", type="password")

            if st.form_submit_button("Login as Service Provider"):
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT id, password_hash, name, service_type FROM service_providers WHERE email = %s", (email,))
                result = cur.fetchone()
                cur.close()
                conn.close()

                if result and verify_password(password, result[1]):
                    st.session_state.user_type = 'service_provider'
                    st.session_state.logged_in_entity_id = result[0] # This is service_provider_id
                    st.session_state.user_name = result[2]
                    st.session_state.service_type = result[3] # e.g., 'ca', 'legal'
                    # No JWT token, direct session state login
                    if 'registered_email' in st.session_state: del st.session_state.registered_email
                    if 'user_type_for_login' in st.session_state: del st.session_state.user_type_for_login
                    st.rerun()
                else:
                    st.error("Invalid email or password for Service Provider.")
    
    st.markdown("---")
    st.write("Don't have an account?")
    col_reg1, col_reg2, col_reg3 = st.columns(3)
    with col_reg1:
        if st.button("Register as Business"):
            st.session_state.show_registration = 'business' # This would ideally switch to a business registration tab/page
            # For now, we assume business registration is part of the original login_page's tab2
            st.info("Business registration is available under the 'Register' tab of Business Login.")
    with col_reg2:
        if st.button("Register as Investor"):
            st.session_state.show_registration = 'investor'
            st.rerun() # Rerun to show the investor registration form
    with col_reg3:
        if st.button("Register as Service Provider"):
            st.session_state.show_registration = 'service_provider'
            st.rerun() # Rerun to show the service provider registration form

def check_auth():
    if 'user_type' in st.session_state:
        if st.session_state.user_type == 'business':
            if 'token' not in st.session_state: return False
            business_id = verify_jwt(st.session_state.token)
            if not business_id: return False
            
            # Validate session token from DB (optional, but good for security)
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute("SELECT 1 FROM user_sessions WHERE session_token = %s AND expires_at > NOW() AND business_id = %s",
                            (st.session_state.token, business_id))
                valid_session = cur.fetchone() is not None
                if not valid_session:
                    # Clean up potentially stale session state if token is invalid/expired in DB
                    logout() # Call your existing logout to clear session
                    return False
                st.session_state.business_id = business_id # Ensure business_id is set from validated token
                st.session_state.logged_in_entity_id = business_id # For consistency
                return True
            except Exception as e:
                st.error(f"Session validation error: {e}") # Log this
                return False
            finally:
                cur.close()
                conn.close()

        elif st.session_state.user_type in ['investor', 'service_provider']:
            # For these types, we are relying on session state directly for now
            # (logged_in_entity_id and user_type must be set)
            return 'logged_in_entity_id' in st.session_state and 'user_name' in st.session_state
    return False

def logout():
    if 'token' in st.session_state:
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            cur.execute(
                "DELETE FROM user_sessions WHERE session_token = %s",
                (st.session_state.token,)
            )
            conn.commit()
        except:
            pass
        finally:
            cur.close()
            conn.close()
    
    for key in list(st.session_state.keys()):
        del st.session_state[key]
def time_ago(dt_object):
    """Converts a datetime object or date object to a 'time ago' string."""
    if dt_object is None:
        return "some time ago"

    now = datetime.now(timezone.utc)

    if isinstance(dt_object, date) and not isinstance(dt_object, datetime):
        # Convert date to datetime (midnight UTC) for comparison
        dt_object = datetime.combine(dt_object, datetime.min.time(), tzinfo=timezone.utc)
    elif isinstance(dt_object, datetime) and dt_object.tzinfo is None:
        # Assume naive datetime is UTC
        dt_object = dt_object.replace(tzinfo=timezone.utc)
    
    if not isinstance(dt_object, datetime) or dt_object.tzinfo is None: # Should be tz-aware by now
        return "invalid date"

    diff = now - dt_object
    
    seconds = diff.total_seconds()
    if seconds < 0: # Future date
        return "in the future" # Or handle as error

    minutes = seconds / 60
    hours = minutes / 60
    days = hours / 24
    
    if seconds < 60:
        return f"{int(seconds)} seconds ago"
    elif minutes < 60:
        return f"{int(minutes)} minutes ago"
    elif hours < 24:
        return f"{int(hours)} hours ago"
    elif days < 7:
        return f"{int(days)} days ago"
    elif days < 30:
        return f"{int(days // 7)} weeks ago"
    elif days < 365:
        return f"{int(days // 30)} months ago"
    else:
        return f"{int(days // 365)} years ago"

def get_quarter_dates(date_obj):
    """Returns (start_date, end_date) for the quarter of date_obj."""
    if isinstance(date_obj, datetime):
        date_obj = date_obj.date()
    ts = pd.Timestamp(date_obj)
    quarter_start = ts.to_period('Q').start_time.date()
    quarter_end = ts.to_period('Q').end_time.date()
    return quarter_start, quarter_end

def get_previous_quarter_dates(date_obj):
    """Returns (start_date, end_date) for the quarter before date_obj's quarter."""
    if isinstance(date_obj, datetime):
        date_obj = date_obj.date()
    ts = pd.Timestamp(date_obj)
    current_quarter_start = ts.to_period('Q').start_time.date()
    previous_quarter_any_day = current_quarter_start - timedelta(days=1)
    return get_quarter_dates(previous_quarter_any_day)

def get_dashboard_financials(business_id, period_start, period_end):
    """Fetches revenue for a given period."""
    conn = get_db_connection()
    cur = conn.cursor()
    revenue = 0.0
    try:
        cur.execute(
            "SELECT SUM(total_amount) FROM invoices WHERE business_id = %s AND issue_date BETWEEN %s AND %s",
            (business_id, period_start, period_end)
        )
        result = cur.fetchone()
        if result and result[0] is not None:
            revenue = float(result[0])
    except Exception as e:
        st.error(f"Error fetching revenue: {e}")
    finally:
        cur.close()
        conn.close()
    return revenue

def get_total_monthly_salary_expense(business_id):
    """Fetches current total monthly salary expense."""
    conn = get_db_connection()
    cur = conn.cursor()
    total_salary = 0.0
    try:
        cur.execute("SELECT SUM(salary) FROM employees WHERE business_id = %s", (business_id,))
        result = cur.fetchone()
        if result and result[0] is not None:
            total_salary = float(result[0])
    except Exception as e:
        st.error(f"Error fetching total salaries: {e}")
    finally:
        cur.close()
        conn.close()
    return total_salary

def get_recent_activities_for_dashboard(business_id, limit=4):
    """Fetches recent activities for the dashboard."""
    activities_data = []
    conn = get_db_connection()
    cur = conn.cursor()

    # 1. New Sale
    try:
        cur.execute(
            """SELECT customer_name, total_amount, issue_date 
               FROM invoices 
               WHERE business_id = %s 
               ORDER BY issue_date DESC, id DESC LIMIT 1""",
            (business_id,)
        )
        sale = cur.fetchone()
        if sale:
            activities_data.append({
                "type": "New Sale",
                "detail": f"To {sale[0]} for ${sale[1]:,.2f}",
                "time_obj": sale[2] 
            })
    except Exception as e:
        st.warning(f"Error fetching new sale activity: {e}")

    # 2. Project Update (from documents)
    try:
        cur.execute(
            """SELECT title, content, created_at 
               FROM documents 
               WHERE business_id = %s AND doc_type = 'project_update' 
               ORDER BY created_at DESC LIMIT 1""",
            (business_id,)
        )
        update = cur.fetchone()
        if update:
            activities_data.append({
                "type": "Project Update",
                "detail": f"{update[0]}: {update[1][:70]}..." if update[1] else update[0],
                "time_obj": update[2]
            })
    except Exception as e:
        st.warning(f"Error fetching project update activity: {e}")
        
    # 3. HR (New Hire)
    try:
        cur.execute(
            """SELECT name, join_date 
               FROM employees 
               WHERE business_id = %s 
               ORDER BY join_date DESC, id DESC LIMIT 1""",
            (business_id,)
        )
        hire = cur.fetchone()
        if hire:
            activities_data.append({
                "type": "HR - New Hire",
                "detail": f"Welcome aboard, {hire[0]}!",
                "time_obj": hire[1]
            })
    except Exception as e:
        st.warning(f"Error fetching HR activity: {e}")

    # 4. Inventory (Low Stock)
    try:
        cur.execute(
            """SELECT name, quantity, created_at 
               FROM products 
               WHERE business_id = %s AND quantity < 10 
               ORDER BY quantity ASC, created_at DESC LIMIT 1""",
            (business_id,)
        )
        low_stock = cur.fetchone()
        if low_stock:
            activities_data.append({
                "type": "Inventory Alert",
                "detail": f"Low stock for {low_stock[0]} (Qty: {low_stock[1]})",
                "time_obj": low_stock[2] # Using product created_at as proxy for event time
            })
    except Exception as e:
        st.warning(f"Error fetching inventory activity: {e}")
    
    cur.close()
    conn.close()

    # Sort activities by time_obj, most recent first
    def get_sort_key(activity):
        time_val = activity.get("time_obj")
        if isinstance(time_val, datetime):
            return time_val
        if isinstance(time_val, date): # Handles datetime.date from DB
            return datetime.combine(time_val, datetime.min.time()).replace(tzinfo=timezone.utc)
        return datetime.min.replace(tzinfo=timezone.utc) # Fallback for None

    activities_data.sort(key=get_sort_key, reverse=True)
    
    # Convert time_obj to "time ago" string for display
    for act in activities_data:
        act["time_string"] = time_ago(act.get("time_obj"))

    return activities_data[:limit]


# Inventory & Billing Module
def inventory_module(business_id, ai_models):
    st.header("📦 Inventory & Billing Management")

    if "invoice_download_details" not in st.session_state:
        st.session_state.invoice_download_details = None
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2, tab3, tab4 = st.tabs(["View Inventory", "Add Product", "Generate Bill", "Reports"])
    
    with tab1:
        cur.execute("SELECT * FROM products WHERE business_id = %s ORDER BY name", (business_id,))
        products = cur.fetchall()
        
        if products:
            df = pd.DataFrame(products, columns=["ID", "Name", "Description", "Price", "Quantity", "Category", "Created At", "Business ID"])
            st.dataframe(df.drop(columns=["Business ID"]), hide_index=True)
            df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
            # Low stock alert
            low_stock = df[df["Quantity"] < 10]
            if not low_stock.empty:
                st.warning("Low Stock Alert!")
                st.dataframe(low_stock[["Name", "Quantity"]])
        else:
            st.info("No products in inventory yet.")
    
    with tab2:
        with st.form("add_product"):
            name = st.text_input("Product Name", key="product_name")
            description = st.text_area("Description", key="product_desc")
            price = st.number_input("Price", min_value=0.0, step=0.01, key="product_price")
            quantity = st.number_input("Quantity", min_value=0, step=1, key="product_qty")
            category = st.text_input("Category", key="product_cat")
            
            if st.form_submit_button("Add Product"):
                cur.execute(
                    "INSERT INTO products (business_id, name, description, price, quantity, category) VALUES (%s, %s, %s, %s, %s, %s)",
                    (business_id, name, description, price, quantity, category)
                )
                conn.commit()
                st.success("Product added successfully!")
                st.rerun()
    
    with tab3:
        with st.form("create_invoice"):
            customer_name = st.text_input("Customer Name")
            customer_email = st.text_input("Customer Email")
            due_date = st.date_input("Due Date", datetime.now() + timedelta(days=14))
            
            # Get products for selection
            cur.execute("SELECT id, name, price FROM products WHERE business_id = %s", (business_id,))
            products = cur.fetchall()
            
            items = []
            if products:
                st.write("### Invoice Items")
                cols = st.columns([3, 2, 2, 1])
                with cols[0]:
                    st.write("**Product**")
                with cols[1]:
                    st.write("**Price**")
                with cols[2]:
                    st.write("**Quantity**")
                with cols[3]:
                    st.write("**Total**")
                
                for i, product in enumerate(products):
                    cols = st.columns([3, 2, 2, 1])
                    with cols[0]:
                        st.write(product[1])
                    with cols[1]:
                        st.write(f"${product[2]:.2f}")
                    with cols[2]:
                        qty = st.number_input(f"Qty {product[1]}", min_value=0, value=0, key=f"qty_{product[0]}")
                    with cols[3]:
                        item_total = product[2] * qty
                        st.write(f"${item_total:.2f}")
                    
                    if qty > 0:
                        items.append({
                            "product_id": product[0],
                            "name": product[1],
                            "price": float(product[2]),
                            "quantity": qty,
                            "total": float(item_total)
                        })
            
            if st.form_submit_button("Create Invoice"):
                if not items:
                    st.error("Please add at least one item to the invoice")
                else:
                    # Calculate totals
                    subtotal = sum(item['total'] for item in items)
                    tax_rate = 0.18  # 18% tax
                    tax_amount = subtotal * tax_rate
                    total_amount = subtotal + tax_amount
                    
                    # Generate invoice number
                    invoice_number = f"INV-{datetime.now().strftime('%Y%m%d')}-{np.random.randint(1000, 9999)}"
                    
                    # Create invoice
                    cur.execute(
                        """INSERT INTO invoices (
                            business_id, invoice_number, customer_name, customer_email,
                            issue_date, due_date, total_amount, tax_amount, items
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            business_id, invoice_number, customer_name, customer_email,
                            datetime.now().date(), due_date, total_amount, tax_amount,
                            json.dumps(items)
                        )
                    )
                    
                    # Update inventory
                    for item in items:
                        cur.execute(
                            "UPDATE products SET quantity = quantity - %s WHERE id = %s AND business_id = %s",
                            (item['quantity'], item['product_id'], business_id)
                        )
                    
                    conn.commit()
                    
                    # Show invoice
                    st.success("Invoice created successfully!")
                    st.subheader(f"Invoice #{invoice_number}")
                    
                    # Download options
                    invoice_content = generate_invoice_pdf(
                        business_id, invoice_number, customer_name, customer_email,
                        datetime.now().date(), due_date, items, subtotal, tax_amount, total_amount
                    )
                    
                    st.session_state.invoice_download_details = {
                        "data": invoice_content,
                        "file_name": f"invoice_{invoice_number}.pdf",
                        "invoice_number": invoice_number
                    }
        if st.session_state.invoice_download_details:
            details = st.session_state.invoice_download_details
            st.subheader(f"Invoice #{details['invoice_number']} Ready for Download")
            st.download_button(
                label="Download Invoice (PDF)",
                data=details["data"],
                file_name=details["file_name"],
                mime="text/plain",
                key="download_invoice_final_btn" # Added a key
            )
            if st.button("Create Another Invoice", key="create_another_inv_btn"):
                st.session_state.invoice_download_details = None
                st.rerun()
    
    with tab4:
        st.subheader("Inventory Reports")
        
        # Sales trends
        st.write("### Sales Trends")
        cur.execute(
            """SELECT DATE_TRUNC('month', issue_date) AS month, 
            SUM(total_amount) AS sales 
            FROM invoices 
            WHERE business_id = %s 
            GROUP BY month 
            ORDER BY month""",
            (business_id,)
        )
        sales_data = cur.fetchall()
        
        if sales_data:
            df_sales = pd.DataFrame(sales_data, columns=["Month", "Sales"])
            fig = px.line(df_sales, x="Month", y="Sales", title="Monthly Sales")
            st.plotly_chart(fig)
        else:
            st.info("No sales data available")
        
        # Inventory value
        st.write("### Inventory Value")
        cur.execute(
            "SELECT category, SUM(price * quantity) AS value FROM products WHERE business_id = %s GROUP BY category",
            (business_id,)
        )
        inv_data = cur.fetchall()
        
        if inv_data:
            df_inv = pd.DataFrame(inv_data, columns=["Category", "Value"])
            fig = px.pie(df_inv, values="Value", names="Category", title="Inventory Value by Category")
            st.plotly_chart(fig)
        else:
            st.info("No inventory data available")
    
    cur.close()
    conn.close()

def generate_invoice_pdf(business_id, invoice_number, customer_name, customer_email, issue_date, due_date, items, subtotal, tax_amount, total_amount):
    # In a real implementation, this would generate an actual PDF
    # For this example, we'll create a simple text representation
    
    invoice_content = f"""
    INVOICE #{invoice_number}
    Issue Date: {issue_date}
    Due Date: {due_date}
    
    From:
    [Your Business Name]
    [Your Business Address]
    
    To:
    {customer_name}
    {customer_email}
    
    ITEMIZED BILL:
    {"Item".ljust(30)} {"Price".ljust(10)} {"Qty".ljust(10)} {"Total".ljust(10)}
    {"-"*60}
    """
    
    for item in items:
        invoice_content += f"\n{item['name'].ljust(30)} ${item['price']:.2f} {str(item['quantity']).ljust(10)} ${item['total']:.2f}"
    
    invoice_content += f"""
    
    SUBTOTAL: ${subtotal:.2f}
    TAX (18%): ${tax_amount:.2f}
    TOTAL: ${total_amount:.2f}
    
    Payment Terms: Due upon receipt
    Payment Methods: [List your payment methods]
    """
    
    # Convert to bytes for download
    return invoice_content.encode()

# HR Tools Module
def hr_module(business_id, ai_models):
    st.header("👥 HR Tools")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Employee Directory", 
        "Appraisals", 
        "Attendance", 
        "Work Reports",
        "Analytics"
    ])
    
    with tab1:
        cur.execute("SELECT * FROM employees WHERE business_id = %s ORDER BY name", (business_id,))
        employees = cur.fetchall()
        
        if employees:
            df = pd.DataFrame(employees, columns=[
                "ID", "Name", "Email", "Position", "Department", 
                "Salary", "Join Date", "Last Appraisal", "Performance", "Skills", "Business ID"
            ])
            st.dataframe(df.drop(columns=["Business ID"]), hide_index=True)
            
            # Search functionality
            search_term = st.text_input("Search Employees")
            if search_term:
                filtered = df[
                    df["Name"].str.contains(search_term, case=False) |
                    df["Email"].str.contains(search_term, case=False) |
                    df["Position"].str.contains(search_term, case=False)
                ]
                st.dataframe(filtered.drop(columns=["Business ID"]), hide_index=True)
        else:
            st.info("No employees in the system yet.")
        
        # Add new employee
        with st.expander("Add New Employee"):
            with st.form("add_employee"):
                name = st.text_input("Full Name")
                email = st.text_input("Email")
                position = st.text_input("Position")
                department = st.text_input("Department")
                salary = st.number_input("Salary", min_value=0, step=1000)
                join_date = st.date_input("Join Date")
                skills = st.text_input("Skills (comma separated)")
                
                if st.form_submit_button("Add Employee"):
                    skills_list = [s.strip() for s in skills.split(",")] if skills else []
                    try:
                        cur.execute(
                            """INSERT INTO employees 
                            (business_id, name, email, position, department, salary, join_date, skills) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            (business_id, name, email, position, department, salary, join_date, skills_list)
                        )
                        conn.commit()
                        st.success("Employee added successfully!")
                        st.rerun()
                    except psycopg2.IntegrityError:
                        st.error("Email already exists for this business")
    
    with tab2:
        st.subheader("Employee Appraisals")
        
        cur.execute("SELECT id, name FROM employees WHERE business_id = %s", (business_id,))
        employees = cur.fetchall()
        
        if employees:
            employee_options = {f"{e[1]} (ID: {e[0]})": e[0] for e in employees}
            selected_employee = st.selectbox("Select Employee", options=list(employee_options.keys()))
            
            if selected_employee:
                employee_id = employee_options[selected_employee]
                cur.execute("SELECT * FROM employees WHERE id = %s AND business_id = %s", (employee_id, business_id))
                emp_data = cur.fetchone()
                
                st.write(f"### Appraisal for {emp_data[2]}")
                st.write(f"**Position:** {emp_data[4]}")
                st.write(f"**Department:** {emp_data[5]}")
                st.write(f"**Current Salary:** ${emp_data[6]:,.2f}")
                st.write(f"**Last Appraisal:** {emp_data[8] if emp_data[8] else 'Never'}")
                st.write(f"**Performance Score:** {emp_data[9] if emp_data[9] else 'Not rated'}/10")
                
                with st.form("appraisal_form"):
                    current_performance_score_db = emp_data[9]
                    default_appraisal_score_for_slider = 5

                    # Determine the effective score based on the original logic
                    # This might result in an int (5), None, or a string (e.g., "7")
                    effective_score_value_from_logic = current_performance_score_db if current_performance_score_db else default_appraisal_score_for_slider

                    # Ensure the value for the slider is an integer
                    try:
                        initial_slider_value_int = int(effective_score_value_from_logic)
                    except (ValueError, TypeError):
                        # This handles cases where effective_score_value_from_logic is a string that cannot be converted to int (e.g., "N/A")
                        # or if it's an unexpected type.
                        st.warning(
                            f"Invalid performance score format ('{effective_score_value_from_logic}') "
                            f"for employee {emp_data[2]} (ID: {emp_data[0]}). "
                            f"Using default value {default_appraisal_score_for_slider} for the slider."
                        )
                        initial_slider_value_int = default_appraisal_score_for_slider
                    
                    new_score = st.slider(
                        "New Performance Score", 
                        min_value=1, 
                        max_value=10, 
                        value=initial_slider_value_int
                    )
                    salary_adjustment = st.number_input("Salary Adjustment (%)", min_value=0.0, max_value=50.0, value=0.0, step=0.5)
                    comments = st.text_area("Appraisal Comments")
                    appraisal_date = st.date_input("Appraisal Date", datetime.now().date())
                    
                    if st.form_submit_button("Submit Appraisal"):
                        new_salary = float(emp_data[5]) * (1 + salary_adjustment/100)
                        cur.execute(
                            """UPDATE employees 
                            SET performance_score = %s, salary = %s, last_appraisal_date = %s 
                            WHERE id = %s AND business_id = %s""",
                            (new_score, new_salary, appraisal_date, employee_id, business_id)
                        )
                        
                        # Add to documents
                        doc_title = f"Appraisal for {emp_data[2]} - {appraisal_date}"
                        doc_content = f"""
                        Employee: {emp_data[2]}
                        Position: {emp_data[4]}
                        Department: {emp_data[5]}
                        
                        Previous Performance Score: {emp_data[9] if emp_data[9] else 'N/A'} → {new_score}
                        
                        Previous Salary: ${emp_data[6]:,.2f}
                        New Salary: ${new_salary:,.2f}
                        Adjustment: {salary_adjustment}%
                        
                        Comments:
                        {comments}
                        """
                        
                        cur.execute(
                            """INSERT INTO documents 
                            (business_id, title, content, doc_type, created_by) 
                            VALUES (%s, %s, %s, %s, %s)""",
                            (business_id, doc_title, doc_content, "appraisal", employee_id)
                        )
                        
                        conn.commit()
                        st.success("Appraisal submitted successfully!")
                        st.rerun()
        else:
            st.info("No employees to appraise")
    
    with tab3:
        st.subheader("Attendance Tracking")
        
        view_option = st.radio("View Mode", ["Daily View", "Employee Summary"])
        
        if view_option == "Daily View":
            date = st.date_input("Select Date", datetime.now().date())
            
            cur.execute("SELECT id, name FROM employees WHERE business_id = %s", (business_id,))
            employees = cur.fetchall()
            
            if employees:
                attendance_data = []
                for emp in employees:
                    # Check existing attendance
                    cur.execute(
                        """SELECT status FROM attendance 
                        WHERE employee_id = %s AND date = %s AND business_id = %s""",
                        (emp[0], date, business_id)
                    )
                    existing = cur.fetchone()
                    
                    status = st.radio(
                        f"{emp[1]}",
                        ["Present", "Absent", "Late", "Leave"],
                        index=0 if not existing else ["Present", "Absent", "Late", "Leave"].index(existing[0]),
                        key=f"att_{emp[0]}_{date}"
                    )
                    
                    if existing:
                        if status != existing[0]:
                            cur.execute(
                                """UPDATE attendance 
                                SET status = %s 
                                WHERE employee_id = %s AND date = %s AND business_id = %s""",
                                (status, emp[0], date, business_id)
                            )
                    else:
                        cur.execute(
                            """INSERT INTO attendance 
                            (business_id, employee_id, date, status) 
                            VALUES (%s, %s, %s, %s)""",
                            (business_id, emp[0], date, status)
                        )
                
                conn.commit()
                st.success("Attendance saved successfully!")
                
                # Show summary
                cur.execute(
                    """SELECT status, COUNT(*) 
                    FROM attendance 
                    WHERE date = %s AND business_id = %s 
                    GROUP BY status""",
                    (date, business_id)
                )
                summary = cur.fetchall()
                
                if summary:
                    df_summary = pd.DataFrame(summary, columns=["Status", "Count"])
                    st.write("### Attendance Summary")
                    st.dataframe(df_summary)
                    
                    fig = px.pie(df_summary, values="Count", names="Status", title=f"Attendance for {date}")
                    st.plotly_chart(fig)
            else:
                st.info("No employees to track attendance for")
        
        else:  # Employee Summary
            cur.execute("SELECT id, name FROM employees WHERE business_id = %s", (business_id,))
            employees = cur.fetchall()
            
            if employees:
                employee_options = {e[1]: e[0] for e in employees}
                selected_employee = st.selectbox("Select Employee", options=list(employee_options.keys()))
                start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30))
                end_date = st.date_input("End Date", datetime.now())
                
                if selected_employee:
                    cur.execute(
                        """SELECT date, status 
                        FROM attendance 
                        WHERE employee_id = %s AND business_id = %s 
                        AND date BETWEEN %s AND %s 
                        ORDER BY date""",
                        (employee_options[selected_employee], business_id, start_date, end_date)
                    )
                    records = cur.fetchall()
                    
                    if records:
                        df_records = pd.DataFrame(records, columns=["Date", "Status"])
                        st.write(f"### Attendance for {selected_employee}")
                        st.dataframe(df_records)
                        
                        # Calculate attendance percentage
                        total_days = (end_date - start_date).days + 1
                        present_days = len([r for r in records if r[1] == "Present"])
                        attendance_percent = (present_days / total_days) * 100
                        
                        st.metric("Attendance Percentage", f"{attendance_percent:.1f}%")
                        
                        # Plot attendance trend
                        df_records['Present'] = df_records['Status'].apply(lambda x: 1 if x == "Present" else 0)
                        df_records.set_index('Date', inplace=True)
                        df_records = df_records.resample('W').sum()
                        
                        fig = px.bar(df_records, y="Present", title="Weekly Attendance")
                        st.plotly_chart(fig)
                    else:
                        st.info("No attendance records found for selected period")
            else:
                st.info("No employees to show attendance for")
    
    with tab4:
        st.subheader("Work Reports")
        
        cur.execute("SELECT id, name FROM employees WHERE business_id = %s", (business_id,))
        employees = cur.fetchall()
        
        if employees:
            employee_id = st.selectbox("Select Employee", options=[f"{e[1]} (ID: {e[0]})" for e in employees], key=business_id)
            report_period = st.selectbox("Report Period", ["Daily", "Weekly", "Monthly"])
            report_date = st.date_input("Report Date", datetime.now().date())
            
            if st.button("Generate Report"):
                # In a real app, this would pull actual data
                report_data = {
                    "tasks_completed": np.random.randint(3, 10),
                    "hours_worked": np.random.randint(4, 9),
                    "meetings_attended": np.random.randint(1, 5),
                    "issues_resolved": np.random.randint(1, 4),
                    "feedback": "Good performance this period. Keep it up!"
                }
                
                st.write(f"### {report_period} Work Report")
                st.write(f"- Tasks Completed: {report_data['tasks_completed']}")
                st.write(f"- Hours Worked: {report_data['hours_worked']}")
                st.write(f"- Meetings Attended: {report_data['meetings_attended']}")
                st.write(f"- Issues Resolved: {report_data['issues_resolved']}")
                st.write(f"- Feedback: {report_data['feedback']}")
                
                if st.button("Save as PDF"):
                    # Generate PDF (simulated)
                    st.success("Report saved to documents!")
        else:
            st.info("No employees to generate reports for")
    
    with tab5:
        st.subheader("HR Analytics Dashboard")
        
        # Employee distribution by department
        cur.execute("SELECT department, COUNT(*) FROM employees WHERE business_id = %s GROUP BY department", (business_id,))
        dept_data = cur.fetchall()
        
        if dept_data:
            df_dept = pd.DataFrame(dept_data, columns=["Department", "Count"])
            fig = px.pie(df_dept, values="Count", names="Department", title="Employees by Department")
            st.plotly_chart(fig)
        else:
            st.info("No department data available")
        
        # Salary distribution
        cur.execute("SELECT position, salary FROM employees WHERE business_id = %s", (business_id,))
        salary_data = cur.fetchall()
        
        if salary_data:
            df_salary = pd.DataFrame(salary_data, columns=["Position", "Salary"])
            fig = px.box(df_salary, y="Salary", title="Salary Distribution")
            st.plotly_chart(fig)
        else:
            st.info("No salary data available")
        
        # Performance vs Salary
        cur.execute("SELECT performance_score, salary FROM employees WHERE business_id = %s AND performance_score IS NOT NULL", (business_id,))
        perf_data = cur.fetchall()
        
        if perf_data:
            df_perf = pd.DataFrame(perf_data, columns=["Performance", "Salary"])
            fig = px.scatter(df_perf, x="Performance", y="Salary", trendline="ols", 
                           title="Performance vs Salary")
            st.plotly_chart(fig)
        else:
            st.info("No performance data available")
    
    cur.close()
    conn.close()

# Project Manager Module
def project_module(business_id, ai_models):
    st.header("📊 Project Manager")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2, tab3, tab4 = st.tabs(["All Projects", "New Project", "Project Details", "Gantt Chart"])
    
    with tab1:
        cur.execute(
            """SELECT p.id, p.name, p.client, p.start_date, p.end_date, p.status, p.progress, e.name as manager 
            FROM projects p LEFT JOIN employees e ON p.manager_id = e.id 
            WHERE p.business_id = %s""",
            (business_id,)
        )
        projects = cur.fetchall()
        
        if projects:
            df = pd.DataFrame(projects, columns=[
                "ID", "Name", "Client", "Start Date", "End Date", 
                "Status", "Progress", "Manager"
            ])
            st.dataframe(df)
            
            # Filter options
            status_filter = st.multiselect(
                "Filter by Status",
                options=df["Status"].unique(),
                default=df["Status"].unique()
            )
            
            if status_filter:
                filtered_df = df[df["Status"].isin(status_filter)]
                st.dataframe(filtered_df)
        else:
            st.info("No projects found.")
    
    with tab2:
        with st.form("new_project"):
            name = st.text_input("Project Name")
            client = st.text_input("Client Name")
            description = st.text_area("Description")
            start_date = st.date_input("Start Date")
            end_date = st.date_input("End Date")
            budget = st.number_input("Budget", min_value=0.0, step=1000.0)
            
            cur.execute("SELECT id, name FROM employees WHERE business_id = %s", (business_id,))
            managers = cur.fetchall()
            manager_options = {m[1]: m[0] for m in managers}
            manager = st.selectbox("Project Manager", options=list(manager_options.keys()))
            
            if st.form_submit_button("Create Project"):
                cur.execute(
                    """INSERT INTO projects 
                    (business_id, name, client, description, start_date, end_date, budget, status, manager_id) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (business_id, name, client, description, start_date, end_date, budget, "Not Started", manager_options[manager])
                )
                conn.commit()
                st.success("Project created successfully!")
                st.rerun()
    
    with tab3:
        cur.execute("SELECT id, name FROM projects WHERE business_id = %s", (business_id,))
        projects = cur.fetchall()
        
        if projects:
            selected_project = st.selectbox(
                "Select Project", 
                options=[f"{p[1]} (ID: {p[0]})" for p in projects]
            )
            
            if selected_project:
                project_id = int(selected_project.split("(ID: ")[1].replace(")", ""))
                cur.execute(
                    """SELECT p.*, e.name as manager_name 
                    FROM projects p LEFT JOIN employees e ON p.manager_id = e.id 
                    WHERE p.id = %s AND p.business_id = %s""",
                    (project_id, business_id)
                )
                project = cur.fetchone()
                
                st.write(f"### {project[2]}")
                st.write(f"**Client:** {project[3]}")
                st.write(f"**Manager:** {project[9]}")
                st.write(f"**Status:** {project[7]}")
                st.write(f"**Progress:** {project[8]}%")
                
                # Project timeline
                today = datetime.now().date()
                start_date = project[4]
                end_date = project[5]
                total_days = (end_date - start_date).days
                days_passed = (today - start_date).days
                progress_percent = min(100, max(0, (days_passed / total_days) * 100)) if total_days > 0 else 0
                
                st.write(f"**Start Date:** {start_date}")
                st.write(f"**End Date:** {end_date}")
                st.write(f"**Days Remaining:** {(end_date - today).days} days")
                
                # Progress bars
                st.progress(project[8] / 100)
                st.caption(f"Project Completion: {project[8]}%")
                
                st.progress(progress_percent / 100)
                st.caption(f"Timeline Progress: {progress_percent:.1f}%")
                
                # Update project status
                with st.expander("Update Project"):
                    with st.form("update_project"):
                        new_status = st.selectbox(
                            "Status",
                            ["Not Started", "In Progress", "On Hold", "Completed", "Cancelled"],
                            index=["Not Started", "In Progress", "On Hold", "Completed", "Cancelled"].index(project[7])
                        )
                        new_progress = st.slider("Progress (%)", 0, 100, project[8])
                        notes = st.text_area("Update Notes")
                        
                        if st.form_submit_button("Update Project"):
                            cur.execute(
                                """UPDATE projects 
                                SET status = %s, progress = %s 
                                WHERE id = %s AND business_id = %s""",
                                (new_status, new_progress, project_id, business_id)
                            )
                            
                            # Add to project documents
                            doc_title = f"Project Update - {project[2]} - {datetime.now().date()}"
                            doc_content = f"""
                            Project: {project[2]}
                            Status Changed: {project[7]} → {new_status}
                            Progress: {project[8]}% → {new_progress}%
                            
                            Notes:
                            {notes}
                            """
                            
                            cur.execute(
                                """INSERT INTO documents 
                                (business_id, title, content, doc_type) 
                                VALUES (%s, %s, %s, %s)""",
                                (business_id, doc_title, doc_content, "project_update")
                            )
                            
                            conn.commit()
                            st.success("Project updated successfully!")
                            st.rerun()
                
                # Project team (simulated)
                st.write("### Project Team")
                cur.execute(
                    """SELECT e.name, e.position 
                    FROM employees e 
                    JOIN project_assignments pa ON e.id = pa.employee_id 
                    WHERE pa.project_id = %s AND e.business_id = %s""",
                    (project_id, business_id)
                )
                team_members = cur.fetchall()
                
                if team_members:
                    st.dataframe(pd.DataFrame(team_members, columns=["Name", "Position"]))
                else:
                    st.info("No team members assigned yet")
                    
                    # Assign team members
                    with st.expander("Assign Team Members"):
                        cur.execute("SELECT id, name FROM employees WHERE business_id = %s", (business_id,))
                        employees = cur.fetchall()
                        
                        if employees:
                            selected_employees = st.multiselect(
                                "Select Employees",
                                options=[f"{e[1]} (ID: {e[0]})" for e in employees]
                            )
                            
                            if st.button("Assign to Project"):
                                for emp in selected_employees:
                                    emp_id = int(emp.split("(ID: ")[1].replace(")", ""))
                                    try:
                                        cur.execute(
                                            """INSERT INTO project_assignments 
                                            (business_id, project_id, employee_id) 
                                            VALUES (%s, %s, %s)""",
                                            (business_id, project_id, emp_id)
                                        )
                                    except psycopg2.IntegrityError:
                                        pass  # Already assigned
                                
                                conn.commit()
                                st.success("Team members assigned successfully!")
                                st.rerun()
                        else:
                            st.info("No employees available to assign")
        else:
            st.info("No projects to show details for")
    
    with tab4:
        st.subheader("Project Gantt Chart")
        
        cur.execute(
            "SELECT name, start_date, end_date, status FROM projects WHERE business_id = %s",
            (business_id,)
        )
        projects = cur.fetchall()
        
        if projects:
            gantt_data = []
            for p in projects:
                gantt_data.append({
                    "Task": p[0],
                    "Start": p[1],
                    "Finish": p[2],
                    "Status": p[3]
                })
            
            df_gantt = pd.DataFrame(gantt_data)
            
            color_map = {
                "Not Started": "#636EFA",
                "In Progress": "#00CC96",
                "On Hold": "#EF553B",
                "Completed": "#AB63FA",
                "Cancelled": "#FFA15A"
            }
            
            fig = px.timeline(
                df_gantt, 
                x_start="Start", 
                x_end="Finish", 
                y="Task",
                color="Status",
                color_discrete_map=color_map,
                title="Project Timeline"
            )
            
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig)
        else:
            st.info("No projects to display")
    
    cur.close()
    conn.close()

# Document Generator Module
def document_module(business_id, ai_models):
    st.header("📝 Document Generator")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2, tab3, tab4 = st.tabs(["Templates", "Generate Document", "Document Library", "AI Assistant"])
    
    with tab1:
        st.subheader("Document Templates")
        
        template_cols = st.columns(3)
        
        with template_cols[0]:
            st.write("**Contract Templates**")
            st.button("Employment Contract")
            st.button("NDA Agreement")
            st.button("Service Contract")
        
        with template_cols[1]:
            st.write("**Business Documents**")
            st.button("Business Proposal")
            st.button("Invoice Template")
            st.button("Meeting Minutes")
        
        with template_cols[2]:
            st.write("**Legal Documents**")
            st.button("Privacy Policy")
            st.button("Terms of Service")
            st.button("Partnership Agreement")
    
    with tab2:
        doc_type = st.selectbox("Document Type", [
            "Contract", "Letter", "Invoice", "Proposal", "Report", "Other"
        ])
        
        doc_title = st.text_input("Document Title")
        
        # Get relevant data based on document type
        if doc_type == "Contract":
            parties = st.text_input("Parties Involved (comma separated)")
            terms = st.text_area("Key Terms")
            duration = st.text_input("Duration")
            termination = st.text_area("Termination Clause")
            
            if st.button("Generate Contract"):
                doc_content = f"""
                CONTRACT AGREEMENT
                
                This Agreement is made and entered into on {datetime.now().date()} by and between:
                
                Parties: {parties}
                
                1. TERMS
                {terms}
                
                2. DURATION
                This agreement shall remain in effect for {duration}.
                
                3. TERMINATION
                {termination}
                
                IN WITNESS WHEREOF, the parties have executed this Agreement as of the date first written above.
                
                ___________________________
                Signature
                """
                
                st.text_area("Generated Document", doc_content, height=400)
                
                # Save to database
                cur.execute(
                    """INSERT INTO documents 
                    (business_id, title, content, doc_type) 
                    VALUES (%s, %s, %s, %s)""",
                    (business_id, f"{doc_type}: {doc_title}", doc_content, doc_type.lower())
                )
                conn.commit()
                
                # Download options
                docx_file = BytesIO()
                doc = docx.Document()
                doc.add_paragraph(doc_content)
                doc.save(docx_file)
                docx_file.seek(0)
                
                st.download_button(
                    "Download as DOCX",
                    data=docx_file,
                    file_name=f"{doc_title}.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                )
        
        elif doc_type == "Invoice":
            cur.execute("SELECT id, name FROM products WHERE business_id = %s", (business_id,))
            products = cur.fetchall()
            
            if products:
                product_options = {p[1]: p[0] for p in products}
                
                client_name = st.text_input("Client Name")
                client_address = st.text_area("Client Address")
                items = st.multiselect("Select Products", options=list(product_options.keys()))
                quantities = [st.number_input(f"Quantity for {item}", min_value=1, value=1, key=f"qty_{item}") for item in items]
                
                if st.button("Generate Invoice"):
                    doc_content = f"""
                    INVOICE
                    Date: {datetime.now().date()}
                    Invoice #: INV-{datetime.now().strftime('%Y%m%d')}-{np.random.randint(1000, 9999)}
                    
                    From:
                    [Your Business Name]
                    [Your Business Address]
                    
                    To:
                    {client_name}
                    {client_address}
                    
                    ITEMS:
                    {"Item".ljust(30)} {"Qty".ljust(10)} {"Rate".ljust(15)} {"Amount".ljust(15)}
                    {"-"*70}
                    """
                    
                    total = 0
                    for item, qty in zip(items, quantities):
                        product_id = product_options[item]
                        cur.execute("SELECT price FROM products WHERE id = %s AND business_id = %s", (product_id, business_id))
                        price = cur.fetchone()[0]
                        amount = price * qty
                        total += amount
                        doc_content += f"\n{item.ljust(30)} {str(qty).ljust(10)} ${price:.2f} ${amount:.2f}"
                    
                    tax_rate = 0.18  # 18% tax
                    tax_amount = total * tax_rate
                    grand_total = total + tax_amount
                    
                    doc_content += f"\n\n{'Subtotal:'.ljust(55)} ${total:.2f}"
                    doc_content += f"\n{'Tax (18%):'.ljust(55)} ${tax_amount:.2f}"
                    doc_content += f"\n{'Total Due:'.ljust(55)} ${grand_total:.2f}"
                    
                    st.text_area("Generated Invoice", doc_content, height=400)
                    
                    # Save to database
                    cur.execute(
                        """INSERT INTO documents 
                        (business_id, title, content, doc_type) 
                        VALUES (%s, %s, %s, %s)""",
                        (business_id, f"Invoice for {client_name}", doc_content, "invoice")
                    )
                    conn.commit()
                    
                    # Download options
                    st.download_button(
                        "Download as TXT",
                        data=doc_content,
                        file_name=f"invoice_{client_name}.txt",
                        mime="text/plain"
                    )
            else:
                st.info("No products available to create invoice")
        
        else:  # Generic document
            prompt = st.text_area("Document Content Prompt", 
                                "Create a professional business document about...")
            
            if st.button("Generate with AI"):
                with st.spinner("Generating document..."):
                    doc_content = ai_models.generate_text(
                        f"Create a {doc_type.lower()} document about: {prompt}",
                        max_length=1000
                    )
                    
                    st.text_area("Generated Document", doc_content, height=400)
                    
                    # Save to database
                    cur.execute(
                        """INSERT INTO documents 
                        (business_id, title, content, doc_type) 
                        VALUES (%s, %s, %s, %s)""",
                        (business_id, f"{doc_type}: {doc_title}", doc_content, doc_type.lower())
                    )
                    conn.commit()
                    
                    # Download options
                    docx_file = BytesIO()
                    doc = docx.Document()
                    doc.add_paragraph(doc_content)
                    doc.save(docx_file)
                    docx_file.seek(0)
                    
                    st.download_button(
                        "Download as DOCX",
                        data=docx_file,
                        file_name=f"{doc_title}.docx",
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    )
    
    with tab3:
        st.subheader("Document Library")
        
        cur.execute(
            "SELECT * FROM documents WHERE business_id = %s ORDER BY created_at DESC",
            (business_id,)
        )
        documents = cur.fetchall()
        
        if documents:
            search_term = st.text_input("Search Documents")
            
            df = pd.DataFrame(documents, columns=[
                "ID", "Title", "Content", "Type", "Created At", "Created By", "Business ID"
            ])

            df['Created At'] = pd.to_datetime(df['Created At'])  

            if search_term:
                df = df[
                    df["Title"].str.contains(search_term, case=False) |
                    df["Content"].str.contains(search_term, case=False)
                ]
            
            for _, row in df.iterrows():
                with st.expander(f"{row['Title']} ({row['Type']}) - {row['Created At'].date()}"):
                    st.write(row["Content"][:500] + "..." if len(row["Content"]) > 500 else row["Content"])
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(
                            "Download as TXT",
                            data=row["Content"],
                            file_name=f"{row['Title']}.txt",
                            mime="text/plain",
                            key=f"txt_{row['ID']}"
                        )
                    with col2:
                        docx_file = BytesIO()
                        doc = docx.Document()
                        doc.add_paragraph(row["Content"])
                        doc.save(docx_file)
                        docx_file.seek(0)
                        
                        st.download_button(
                            "Download as DOCX",
                            data=docx_file,
                            file_name=f"{row['Title']}.docx",
                            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                            key=f"docx_{row['ID']}"
                        )
        else:
            st.info("No documents in the library yet.")
    
    with tab4:
        st.subheader("Document AI Assistant")
        
        if "doc_chat_history" not in st.session_state:
            st.session_state.doc_chat_history = []
        
        user_input = st.text_input("Ask about documents or request edits:")
        
        if user_input:
            with st.spinner("Generating response..."):
                # In a real app, this would analyze the document library
                response = ai_models.generate_response(
                    f"You are a document assistant. The user asked: {user_input}. "
                    "Provide helpful advice about creating, editing, or managing business documents."
                )
                
                st.session_state.doc_chat_history.append(("You", user_input))
                st.session_state.doc_chat_history.append(("AI Assistant", response))
        
        for speaker, text in st.session_state.doc_chat_history:
            if speaker == "You":
                st.markdown(f"**You**: {text}")
            else:
                st.markdown(f"**AI Assistant**: {text}")
                st.write("---")
    
    cur.close()
    conn.close()

# Market Analysis Module
def market_analysis_module(business_id, ai_models):
    st.header("📈 Market Analysis Tool")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "Industry Analysis", 
        "Trend Insights", 
        "Competitor Benchmark", 
        "Forecasting"
    ])
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    with tab1:
        industry = st.selectbox("Select Industry", [
            "Technology", "Healthcare", "Finance", "Retail", 
            "Manufacturing", "Education", "Real Estate", "Hospitality"
        ])
        
        if st.button("Analyze Industry"):
            with st.spinner(f"Analyzing {industry} industry..."):
                time.sleep(2)  # Simulate analysis
                
                # Generate fake analysis (in a real app, this would call an API)
                trends = [
                    f"Growing demand for {industry.lower()} solutions in emerging markets",
                    f"Increased investment in {industry.lower()} automation",
                    f"Regulatory changes affecting {industry.lower()} sector"
                ]
                
                # Market size data (fake)
                market_size = round(np.random.uniform(1, 100), 2)
                growth_rate = round(np.random.uniform(1, 20), 2)
                
                st.subheader(f"{industry} Industry Overview")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Market Size (USD Billion)", f"${market_size}")
                with col2:
                    st.metric("Annual Growth Rate", f"{growth_rate}%")
                
                st.write("### Key Trends")
                for trend in trends:
                    st.write(f"- {trend}")
                
                # Sentiment analysis example
                st.write("### Market Sentiment")
                sample_reviews = [
                    f"Positive growth outlook for {industry} companies",
                    f"Challenges in {industry} supply chains",
                    f"Investors bullish on {industry} startups"
                ]
                
                sentiments = ai_models.sentiment_analyzer(sample_reviews)
                
                for review, sentiment in zip(sample_reviews, sentiments):
                    label = sentiment['label']
                    score = sentiment['score']
                    st.write(f"- {review} ({label}, {score:.2f} confidence)")
                
                # Save analysis to database
                cur.execute(
                    """INSERT INTO market_data 
                    (business_id, industry, metric, value, date, source) 
                    VALUES (%s, %s, %s, %s, %s, %s)""",
                    (business_id, industry, "market_size", market_size, datetime.now().date(), "GrowBis Analysis")
                )
                cur.execute(
                    """INSERT INTO market_data 
                    (business_id, industry, metric, value, date, source) 
                    VALUES (%s, %s, %s, %s, %s, %s)""",
                    (business_id, industry, "growth_rate", growth_rate, datetime.now().date(), "GrowBis Analysis")
                )
                conn.commit()
    
    with tab2:
        st.subheader("Real-time Market Insights")
        
        # Get trending topics from news API (simulated)
        trending_topics = [
            "Sustainable business practices gaining traction",
            "Remote work tools see continued growth",
            "Supply chain disruptions easing in Q3",
            "AI adoption accelerating across sectors"
        ]
        
        st.write("### Trending Topics")
        for topic in trending_topics:
            st.write(f"- {topic}")
        
        # Industry news
        st.write("### Industry News")
        news_items = [
            {
                "title": "Tech sector leads Q2 earnings",
                "summary": "Major tech companies report strong earnings despite economic headwinds",
                "impact": "Positive",
                "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            },
            {
                "title": "New regulations for fintech",
                "summary": "Government announces new compliance requirements for financial technology firms",
                "impact": "Negative",
                "date": (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
            },
            {
                "title": "Global retail sales rebound",
                "summary": "Consumer spending shows signs of recovery after seasonal slump",
                "impact": "Positive",
                "date": (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
            }
        ]
        
        for news in news_items:
            with st.expander(f"{news['date']}: {news['title']} ({news['impact']})"):
                st.write(news['summary'])
    
    with tab3:
        st.subheader("Competitor Benchmarking")
        
        # Get business's competitors (in a real app, this would be from database)
        competitors = st.text_area("Enter your main competitors (comma separated)")
        
        if competitors:
            competitor_list = [c.strip() for c in competitors.split(",")]
            
            # Simulated competitor data
            competitor_data = []
            for comp in competitor_list:
                competitor_data.append({
                    "name": comp,
                    "market_share": round(np.random.uniform(5, 30), 1),
                    "growth": round(np.random.uniform(-5, 20), 1),
                    "strengths": ["Brand recognition", "Distribution network"],
                    "weaknesses": ["High costs", "Slow innovation"]
                })
            
            # Market share chart
            df_competitors = pd.DataFrame([
                {"Competitor": c["name"], "Market Share (%)": c["market_share"]} 
                for c in competitor_data
            ])
            
            fig = px.bar(
                df_competitors, 
                x="Competitor", 
                y="Market Share (%)", 
                title="Market Share Comparison"
            )
            st.plotly_chart(fig)
            
            # Competitor details
            selected_competitor = st.selectbox(
                "Select Competitor", 
                options=[c["name"] for c in competitor_data]
            )
            
            if selected_competitor:
                comp = next(c for c in competitor_data if c["name"] == selected_competitor)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Market Share", f"{comp['market_share']}%")
                with col2:
                    st.metric("YoY Growth", f"{comp['growth']}%")
                
                st.write("### Strengths")
                for strength in comp["strengths"]:
                    st.write(f"- {strength}")
                
                st.write("### Weaknesses")
                for weakness in comp["weaknesses"]:
                    st.write(f"- {weakness}")
        else:
            st.info("Please enter your competitors to begin analysis")
    
    with tab4:
        st.subheader("Market Forecasting")
        
        # Get business's products/services for forecasting
        cur.execute(
            "SELECT name FROM products WHERE business_id = %s",
            (business_id,)
        )
        products = [p[0] for p in cur.fetchall()]
        
        if products:
            selected_product = st.selectbox("Select Product for Forecast", products)
            forecast_period = st.selectbox("Forecast Period", ["3 months", "6 months", "1 year"])
            
            if st.button("Generate Forecast"):
                with st.spinner("Generating market forecast..."):
                    time.sleep(3)  # Simulate analysis
                    
                    # Generate forecast using AI
                    forecast = ai_models.generate_text(
                        f"Create a {forecast_period} market forecast for {selected_product}. "
                        "Include growth projections, risks, and recommendations.",
                        max_length=1500
                    )
                    
                    st.subheader(f"Market Forecast for {selected_product}")
                    st.write(forecast)
                    
                    # Simulated forecast chart
                    if forecast_period == "3 months":
                        periods = ["Month 1", "Month 2", "Month 3"]
                    elif forecast_period == "6 months":
                        periods = ["Month 1", "Month 2", "Month 3", "Month 4", "Month 5", "Month 6"]
                    else:
                        periods = [f"Q{quarter}" for quarter in range(1, 5)]
                    
                    forecast_values = np.random.normal(loc=100, scale=20, size=len(periods)).cumsum()
                    
                    fig = px.line(
                        x=periods,
                        y=forecast_values,
                        title=f"{forecast_period} Sales Forecast",
                        labels={"x": "Period", "y": "Projected Sales"}
                    )
                    st.plotly_chart(fig)
                    
                    # Save forecast to database
                    cur.execute(
                        """INSERT INTO market_data 
                        (business_id, industry, metric, value, date, source) 
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                        (business_id, "Product", f"{selected_product}_forecast", float(forecast_values[-1]), datetime.now().date(), "GrowBis Forecast")
                    )
                    conn.commit()
        else:
            st.info("No products available for forecasting")
    
    cur.close()
    conn.close()

# AI Chatbot Module
def chatbot_module(business_id, ai_models):
    st.header("🤖 Market Doubt Assistant")
    
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    
    # Voice input option
    voice_input = st.checkbox("Use Voice Input")
    user_input = ""
    
    if voice_input:
        r = sr.Recognizer()
        with st.expander("Click to Record"):
            if st.button("Start Recording"):
                with st.spinner("Recording... Speak now"):
                    with sr.Microphone() as source:
                        audio = r.listen(source)
                        try:
                            user_input = r.recognize_google(audio)
                            st.text_area("You said", user_input)
                        except Exception as e:
                            st.error(f"Error recognizing speech: {e}")
    else:
        user_input = st.text_input("Ask any business, product, or market trend question:")
    
    if user_input:
        with st.spinner("Generating response..."):
            response = ai_models.generate_response(user_input)
            st.session_state.chat_history.append(("You", user_input))
            st.session_state.chat_history.append(("AI", response))
    
    for speaker, text in st.session_state.chat_history:
        if speaker == "You":
            st.markdown(f"**You**: {text}")
        else:
            st.markdown(f"**AI Assistant**: {text}")
            
            # Text-to-speech for responses
            if st.button("🔊 Play", key=f"tts_{hash(text)}"):
                tts = gTTS(text=text, lang='en')
                audio_file = BytesIO()
                tts.write_to_fp(audio_file)
                audio_file.seek(0)
                
                st.audio(audio_file, format='audio/mp3')
            
            st.write("---")

# Investor & Agent Dashboards
def investor_dashboard(business_id, ai_models):
    st.header("💰 Investor & Agent Dashboards")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2, tab3 = st.tabs(["Investor Directory", "Portfolio Analytics", "Deal Flow"])
    
    with tab1:
        cur.execute("SELECT * FROM investors WHERE business_id = %s", (business_id,))
        investors = cur.fetchall()
        
        if investors:
            df = pd.DataFrame(investors, columns=[
                "ID", "Name", "Firm", "Email", "Investment Focus", 
                "Portfolio Companies", "Last Contact", "Business ID"
            ])
            st.dataframe(df.drop(columns=["Business ID"]), hide_index=True)
        else:
            st.info("No investors in database yet.")
        
        # Add new investor
        with st.expander("Add New Investor"):
            with st.form("add_investor"):
                name = st.text_input("Investor Name")
                firm = st.text_input("Firm")
                email = st.text_input("Email")
                focus = st.text_input("Investment Focus")
                companies = st.text_input("Portfolio Companies (comma separated)")
                last_contact = st.date_input("Last Contact Date", datetime.now().date())
                
                if st.form_submit_button("Add Investor"):
                    companies_list = [c.strip() for c in companies.split(",")] if companies else []
                    cur.execute(
                        """INSERT INTO investors 
                        (business_id, name, firm, email, investment_focus, portfolio_companies, last_contact) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, name, firm, email, focus, companies_list, last_contact)
                    )
                    conn.commit()
                    st.success("Investor added successfully!")
                    st.rerun()
    
    with tab2:
        st.subheader("Portfolio Analytics")
        
        cur.execute("SELECT * FROM investors WHERE business_id = %s", (business_id,))
        investors = cur.fetchall()
        
        if investors:
            # Portfolio composition
            df = pd.DataFrame(investors, columns=[
                "ID", "Name", "Firm", "Email", "Investment Focus", 
                "Portfolio Companies", "Last Contact", "Business ID"
            ])
            st.write("### Portfolio Composition")
            
            # Get all portfolio companies
            all_companies = []
            for inv in df.iterrows():
                portfolio_companies_list = inv[1]["Portfolio Companies"] 
                if portfolio_companies_list: # Checks for None and non-empty
                    if isinstance(portfolio_companies_list, list):
                        all_companies.extend(portfolio_companies_list)
                    else:
                        # Optional: Log or display a warning if the data type is unexpected
                        st.warning(f"Investor ID {inv[0]}: 'portfolio_companies' field (expected list) has type {type(portfolio_companies_list)}.")
            
            
            if all_companies:
                df_companies = pd.DataFrame({"Company": all_companies})
                company_counts = df_companies["Company"].value_counts().reset_index()
                company_counts.columns = ["Sector", "Count"]
                
                fig = px.pie(
                    company_counts, 
                    values="Count", 
                    names="Sector", 
                    title="Portfolio Companies by Sector"
                )
                st.plotly_chart(fig)
            else:
                st.info("No portfolio company data available.")
            
            # Investment focus distribution
            st.write("### Investment Focus Distribution")
            focus_counts = df["Investment Focus"].value_counts().reset_index()
            focus_counts.columns = ["Focus Area", "Count"]
            
            fig = px.bar(
                focus_counts, 
                x="Focus Area", 
                y="Count", 
                title="Investor Focus Areas"
            )
            st.plotly_chart(fig)
        else:
            st.info("No investor data available for analytics.")
    
    with tab3:
        st.subheader("Deal Flow Management")
        
        # Add new deal
        with st.expander("Add New Deal"):
            with st.form("add_deal"):
                company = st.text_input("Company Name")
                stage = st.selectbox("Deal Stage", [
                    "Initial Contact", "Pitch Meeting", "Due Diligence", 
                    "Term Sheet", "Closed"
                ])
                amount = st.number_input("Potential Amount", min_value=0, step=1000)
                investor = st.text_input("Investor Contact")
                next_step = st.text_input("Next Step")
                target_date = st.date_input("Target Date")
                
                if st.form_submit_button("Add Deal"):
                    # In a real app, this would save to a deals table
                    st.success("Deal added to pipeline!")
        
        # Simulated deal flow pipeline
        stages = ["Initial Contact", "Pitch Meeting", "Due Diligence", "Term Sheet", "Closed"]
        deals = [
            {
                "company": "TechStart",
                "stage": "Initial Contact",
                "amount": 500000,
                "contact": "Sarah Johnson",
                "next_step": "Schedule pitch meeting",
                "date": (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
            },
            {
                "company": "DataAI",
                "stage": "Pitch Meeting",
                "amount": 1000000,
                "contact": "Michael Chen",
                "next_step": "Send follow-up materials",
                "date": (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d")
            },
            {
                "company": "CloudScale",
                "stage": "Due Diligence",
                "amount": 2000000,
                "contact": "David Wilson",
                "next_step": "Review financials",
                "date": (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            }
        ]
        
        # Deal flow pipeline visualization
        df_deals = pd.DataFrame(deals)
        fig = px.funnel(
            df_deals, 
            x="amount", 
            y="stage", 
            color="company",
            title="Deal Flow Pipeline"
        )
        st.plotly_chart(fig)
        
        # Deal details
        st.write("### Deal Details")
        for deal in deals:
            with st.expander(f"{deal['company']} - {deal['stage']}"):
                st.write(f"**Amount:** ${deal['amount']:,}")
                st.write(f"**Investor Contact:** {deal['contact']}")
                st.write(f"**Next Step:** {deal['next_step']}")
                st.write(f"**Target Date:** {deal['date']}")
    
    cur.close()
    conn.close()

# Govt/Private Schemes Module
def schemes_module(business_id, ai_models):
    st.header("🏛️ Govt/Private Schemes & News Alerts")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2 = st.tabs(["Available Schemes", "News Alerts"])
    
    with tab1:
        cur.execute("SELECT * FROM schemes WHERE business_id = %s", (business_id,))
        schemes = cur.fetchall()
        
        if schemes:
            df = pd.DataFrame(schemes, columns=[
                "ID", "Name", "Description", "Eligibility", 
                "Benefits", "Deadline", "Sector", "Is Govt", "Business ID"
            ])
            
            # Filter options
            sector_filter = st.multiselect(
                "Filter by Sector",
                options=df["Sector"].unique(),
                default=df["Sector"].unique()
            )
            
            govt_filter = st.checkbox("Government Schemes Only", value=True)
            
            # Apply filters
            filtered_df = df[df["Sector"].isin(sector_filter)]
            if govt_filter:
                filtered_df = filtered_df[filtered_df["Is Govt"] == True]
            
            # Display schemes
            for _, row in filtered_df.iterrows():
                with st.expander(f"{row['Name']} ({'Govt' if row['Is Govt'] else 'Private'}) - Deadline: {row['Deadline']}"):
                    st.write(f"**Sector:** {row['Sector']}")
                    st.write(f"**Description:** {row['Description']}")
                    st.write(f"**Eligibility:** {row['Eligibility']}")
                    st.write(f"**Benefits:** {row['Benefits']}")
                    
                    days_left = (row['Deadline'] - datetime.now().date()).days
                    if days_left > 0:
                        st.warning(f"⏰ {days_left} days left to apply")
                    else:
                        st.error("❌ Deadline passed")
                    
                    if st.button("Apply Now", key=f"apply_{row['ID']}"):
                        st.info("Application form would open here in a real app")
        else:
            st.info("No schemes in database yet.")
        
        # Add new scheme
        with st.expander("Add New Scheme"):
            with st.form("add_scheme"):
                name = st.text_input("Scheme Name")
                description = st.text_area("Description")
                eligibility = st.text_area("Eligibility Criteria")
                benefits = st.text_area("Benefits")
                deadline = st.date_input("Deadline")
                sector = st.text_input("Sector")
                is_govt = st.checkbox("Government Scheme", value=True)
                
                if st.form_submit_button("Add Scheme"):
                    cur.execute(
                        """INSERT INTO schemes 
                        (business_id, name, description, eligibility, benefits, deadline, sector, is_govt) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, name, description, eligibility, benefits, deadline, sector, is_govt)
                    )
                    conn.commit()
                    st.success("Scheme added successfully!")
                    st.rerun()
    
    with tab2:
        st.subheader("Latest Business News Alerts")
        
        # Simulated news alerts
        alerts = [
            {
                "title": "New Export Promotion Scheme Announced",
                "summary": "Government launches scheme to boost exports in manufacturing sector",
                "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "priority": "High"
            },
            {
                "title": "Tax Incentives for R&D Investments",
                "summary": "New policy offers 150% deduction on R&D spending for eligible businesses",
                "date": (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M"),
                "priority": "Medium"
            },
            {
                "title": "Rural Business Grant Program",
                "summary": "Applications open for businesses operating in rural areas",
                "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M"),
                "priority": "Low"
            }
        ]
        
        for alert in alerts:
            with st.expander(f"{alert['date']}: {alert['title']} ({alert['priority']} Priority)"):
                st.write(alert['summary'])
                if st.button("Learn More", key=f"alert_{alert['title']}"):
                    st.info("More details would appear here in a real app")
    
    cur.close()
    conn.close()

# Opportunity Director Module
def opportunities_module(business_id, ai_models):
    st.header("🎯 Opportunity Director")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2, tab3 = st.tabs(["Business Leads", "Grants", "Competitions"])
    
    with tab1:
        st.subheader("Business Leads")
        
        cur.execute("SELECT * FROM opportunities WHERE business_id = %s AND category = 'lead'", (business_id,))
        leads = cur.fetchall()
        
        if leads:
            for lead in leads:
                with st.expander(f"{lead[2]} (Deadline: {lead[5]})"):
                    st.write(lead[3])  # description
                    st.write(f"**Reward:** {lead[6]}")
                    st.write(f"**Link:** {lead[7]}")
                    
                    days_left = (lead[5] - datetime.now().date()).days
                    if days_left > 0:
                        st.warning(f"⏰ {days_left} days left")
                    else:
                        st.error("❌ Expired")
        else:
            st.info("No business leads found.")
        
        # Add new lead
        with st.expander("Add New Lead"):
            with st.form("add_lead"):
                title = st.text_input("Lead Title")
                description = st.text_area("Description")
                reward = st.text_input("Potential Reward")
                deadline = st.date_input("Deadline")
                link = st.text_input("Link")
                
                if st.form_submit_button("Add Lead"):
                    cur.execute(
                        """INSERT INTO opportunities 
                        (business_id, title, description, category, deadline, reward, link) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, title, description, "lead", deadline, reward, link)
                    )
                    conn.commit()
                    st.success("Lead added successfully!")
                    st.rerun()
    
    with tab2:
        st.subheader("Grant Opportunities")
        
        cur.execute("SELECT * FROM opportunities WHERE business_id = %s AND category = 'grant'", (business_id,))
        grants = cur.fetchall()
        
        if grants:
            for grant in grants:
                with st.expander(f"{grant[2]} (Deadline: {grant[5]})"):
                    st.write(grant[3])  # description
                    st.write(f"**Amount:** {grant[6]}")
                    st.write(f"**Link:** {grant[7]}")
                    
                    days_left = (grant[5] - datetime.now().date()).days
                    if days_left > 0:
                        st.warning(f"⏰ {days_left} days left to apply")
                    else:
                        st.error("❌ Deadline passed")
        else:
            st.info("No grant opportunities found.")
        
        # Add new grant
        with st.expander("Add New Grant"):
            with st.form("add_grant"):
                title = st.text_input("Grant Name")
                description = st.text_area("Description")
                amount = st.text_input("Grant Amount")
                deadline = st.date_input("Deadline")
                link = st.text_input("Application Link")
                
                if st.form_submit_button("Add Grant"):
                    cur.execute(
                        """INSERT INTO opportunities 
                        (business_id, title, description, category, deadline, reward, link) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, title, description, "grant", deadline, amount, link)
                    )
                    conn.commit()
                    st.success("Grant opportunity added successfully!")
                    st.rerun()
    
    with tab3:
        st.subheader("Business Competitions")
        
        cur.execute("SELECT * FROM opportunities WHERE business_id = %s AND category = 'competition'", (business_id,))
        competitions = cur.fetchall()
        
        if competitions:
            for comp in competitions:
                with st.expander(f"{comp[2]} (Deadline: {comp[5]})"):
                    st.write(comp[3])  # description
                    st.write(f"**Prize:** {comp[6]}")
                    st.write(f"**Link:** {comp[7]}")
                    
                    days_left = (comp[5] - datetime.now().date()).days
                    if days_left > 0:
                        st.warning(f"⏰ {days_left} days left to enter")
                    else:
                        st.error("❌ Registration closed")
        else:
            st.info("No competitions found.")
        
        # Add new competition
        with st.expander("Add New Competition"):
            with st.form("add_competition"):
                title = st.text_input("Competition Name")
                description = st.text_area("Description")
                prize = st.text_input("Prize")
                deadline = st.date_input("Deadline")
                link = st.text_input("Registration Link")
                
                if st.form_submit_button("Add Competition"):
                    cur.execute(
                        """INSERT INTO opportunities 
                        (business_id, title, description, category, deadline, reward, link) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, title, description, "competition", deadline, prize, link)
                    )
                    conn.commit()
                    st.success("Competition added successfully!")
                    st.rerun()
    
    cur.close()
    conn.close()

# Voice Navigation Module
def voice_navigation(business_id, ai_models):
    st.header("🎙️ Voice Navigation")
    
    r = sr.Recognizer()
    
    st.write("Click the button below and speak your command:")
    
    if st.button("Start Voice Command"):
        with st.spinner("Listening... Speak now"):
            try:
                with sr.Microphone() as source:
                    audio = r.listen(source, timeout=5)
                    command = r.recognize_google(audio)
                    
                    st.success(f"You said: {command}")
                    
                    # Process command
                    if "inventory" in command.lower():
                        st.session_state.nav_module = "Inventory & Billing"
                    elif "hr" in command.lower() or "human resources" in command.lower():
                        st.session_state.nav_module = "HR Tools"
                    elif "project" in command.lower():
                        st.session_state.nav_module = "Project Manager"
                    elif "market" in command.lower():
                        st.session_state.nav_module = "Market Analysis Tool"
                    elif "chat" in command.lower() or "assistant" in command.lower():
                        st.session_state.nav_module = "Market Doubt Assistant (AI Chatbot)"
                    elif "investor" in command.lower():
                        st.session_state.nav_module = "Investor & Agent Dashboards"
                    elif "scheme" in command.lower() or "grant" in command.lower():
                        st.session_state.nav_module = "Govt/Private Schemes & News Alerts"
                    elif "opportunity" in command.lower() or "lead" in command.lower():
                        st.session_state.nav_module = "Opportunity Director"
                    else:
                        st.info("Module not recognized. Please try again.")
                    
                    if "nav_module" in st.session_state:
                        st.info(f"Navigating to: {st.session_state.nav_module}")
            except sr.UnknownValueError:
                st.error("Could not understand audio")
            except sr.RequestError as e:
                st.error(f"Could not request results; {e}")
            except Exception as e:
                st.error(f"Error: {e}")

# Pitching Helper Module
def pitching_helper(business_id, ai_models):
    st.header("📢 Pitching Helper")
    
    tab1, tab2, tab3 = st.tabs(["Pitch Deck Generator", "Funding Scripts", "Investor Prep"])
    
    with tab1:
        st.subheader("AI-Crafted Pitch Decks")
        
        with st.form("pitch_deck_input"):
            company_name = st.text_input("Company Name")
            business_description = st.text_area("Business Description")
            problem = st.text_area("Problem Statement")
            solution = st.text_area("Your Solution")
            market_size = st.text_input("Market Size")
            business_model = st.text_input("Business Model")
            funding_amount = st.text_input("Funding Amount Sought")
            
            if st.form_submit_button("Generate Pitch Deck"):
                with st.spinner("Creating your pitch deck..."):
                    # Generate slides content
                    prompt = f"""
                    Create a pitch deck for {company_name} with the following details:
                    
                    Business: {business_description}
                    Problem: {problem}
                    Solution: {solution}
                    Market: {market_size}
                    Model: {business_model}
                    Funding: {funding_amount}
                    
                    Include 10 slides with titles and bullet points.
                    """
                    
                    deck_content = ai_models.generate_text(prompt, max_length=1500)
                    
                    # Display generated content
                    st.subheader("Generated Pitch Deck Outline")
                    st.write(deck_content)
                    
                    # Create downloadable doc
                    doc = docx.Document()
                    doc.add_heading(f"{company_name} Pitch Deck", 0)
                    
                    # Parse slides from generated content
                    slides = [s for s in deck_content.split("\n\n") if s.strip()]
                    for slide in slides:
                        if slide.startswith("Slide") or ":" in slide:
                            title = slide.split(":")[0] if ":" in slide else slide
                            doc.add_heading(title, level=1)
                            content = slide.split(":")[1] if ":" in slide else ""
                            doc.add_paragraph(content)
                        else:
                            doc.add_paragraph(slide)
                    
                    # Save to buffer
                    docx_file = BytesIO()
                    doc.save(docx_file)
                    docx_file.seek(0)
                    
                    st.download_button(
                        "Download Pitch Deck",
                        data=docx_file,
                        file_name=f"{company_name}_Pitch_Deck.docx",
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    )
    
    with tab2:
        st.subheader("Funding Script Generator")
        
        with st.form("funding_script_input"):
            investor_type = st.selectbox("Investor Type", [
                "Angel Investor", "VC Firm", "Corporate Investor", "Crowdfunding"
            ])
            meeting_type = st.selectbox("Meeting Type", [
                "Initial Pitch", "Follow-up", "Due Diligence", "Term Negotiation"
            ])
            key_points = st.text_area("Key Points to Cover")
            
            if st.form_submit_button("Generate Script"):
                with st.spinner("Creating your funding script..."):
                    prompt = f"""
                    Create a funding conversation script for a {meeting_type} meeting with {investor_type}.
                    
                    Key points to cover:
                    {key_points}
                    
                    Include introduction, main points, responses to likely questions, and closing.
                    """
                    
                    script = ai_models.generate_text(prompt, max_length=1000)
                    
                    st.subheader("Generated Funding Script")
                    st.write(script)
                    
                    st.download_button(
                        "Download Script",
                        data=script,
                        file_name=f"Funding_Script_{investor_type.replace(' ', '_')}.txt",
                        mime="text/plain"
                    )
    
    with tab3:
        st.subheader("Investor Preparation")
        
        st.write("### Common Investor Questions")
        questions = [
            "What problem are you solving?",
            "How big is the market opportunity?",
            "What makes your solution unique?",
            "What's your business model?",
            "What's your customer acquisition strategy?",
            "Who are your competitors?",
            "What are the key risks?",
            "What's your funding ask and how will you use it?",
            "What's your exit strategy?"
        ]
        
        for q in questions:
            with st.expander(q):
                answer = ai_models.generate_text(
                    f"How should a startup answer the investor question: {q}",
                    max_length=300
                )
                st.write(answer)
        
        st.write("### Practice Pitch Session")
        if st.button("Start Mock Pitch Session"):
            st.info("In a real app, this would simulate a pitch session with AI feedback")

# Strategy Generator Module
def strategy_generator(business_id, ai_models):
    st.header("♟️ Strategy Generator")
    
    with st.form("strategy_input"):
        business_type = st.text_input("Business Type")
        business_stage = st.selectbox("Business Stage", [
            "Ideation", "Early-stage", "Growth", "Mature"
        ])
        challenges = st.text_area("Key Challenges")
        goals = st.text_area("Short-term Goals (3-6 months)")
        long_term_goals = st.text_area("Long-term Goals (1-3 years)")
        
        if st.form_submit_button("Generate Growth Strategy"):
            with st.spinner("Creating your personalized growth playbook..."):
                prompt = f"""
                Create a growth strategy for a {business_stage} stage {business_type} business.
                
                Challenges:
                {challenges}
                
                Short-term Goals:
                {goals}
                
                Long-term Goals:
                {long_term_goals}
                
                Provide a detailed playbook with initiatives, timelines, and success metrics.
                """
                
                strategy = ai_models.generate_text(prompt, max_length=2000)
                
                st.subheader("Your Growth Playbook")
                st.write(strategy)
                
                # Create sections
                sections = strategy.split("\n\n")
                for section in sections:
                    if section.strip():
                        with st.expander(section.split("\n")[0][:50] + "..." if len(section) > 50 else section):
                            st.write(section)
                
                st.download_button(
                    "Download Playbook",
                    data=strategy,
                    file_name=f"{business_type}_Growth_Playbook.txt",
                    mime="text/plain"
                )

# Hiring Helper Module
def hiring_helper(business_id, ai_models):
    st.header("👔 Hiring Helper")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2, tab3 = st.tabs([
        "Job Openings", 
        "JD Generator", 
        "Onboarding Toolkit"
    ])
    
    with tab1:
        st.subheader("Manage Job Openings")
        
        cur.execute("SELECT * FROM job_openings WHERE business_id = %s", (business_id,))
        jobs = cur.fetchall()
        
        if jobs:
            df = pd.DataFrame(jobs, columns=[
                "ID", "Title", "Department", "Description", 
                "Requirements", "Experience", "Posted Date", "Status", "Business ID"
            ])
            st.dataframe(df.drop(columns=["Business ID"]), hide_index=True)
        else:
            st.info("No job openings posted yet.")
        
        # Add new job opening
        with st.expander("Post New Job Opening"):
            with st.form("add_job"):
                title = st.text_input("Job Title")
                department = st.text_input("Department")
                description = st.text_area("Job Description")
                requirements = st.text_input("Requirements (comma separated)")
                experience = st.text_input("Experience Needed")
                status = st.selectbox("Status", ["Active", "Closed", "On Hold"])
                
                if st.form_submit_button("Post Job"):
                    req_list = [r.strip() for r in requirements.split(",")] if requirements else []
                    cur.execute(
                        """INSERT INTO job_openings 
                        (business_id, title, department, description, requirements, experience_needed, posted_date, status) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, title, department, description, req_list, experience, datetime.now().date(), status)
                    )
                    conn.commit()
                    st.success("Job posted successfully!")
                    st.rerun()
    
    with tab2:
        st.subheader("Job Description Generator")
        
        with st.form("jd_generator"):
            job_title = st.text_input("Job Title")
            department = st.text_input("Department")
            key_responsibilities = st.text_area("Key Responsibilities")
            qualifications = st.text_area("Required Qualifications")
            preferred_skills = st.text_area("Preferred Skills")
            
            if st.form_submit_button("Generate JD"):
                with st.spinner("Creating professional job description..."):
                    prompt = f"""
                    Create a professional job description for a {job_title} in the {department} department.
                    
                    Key Responsibilities:
                    {key_responsibilities}
                    
                    Required Qualifications:
                    {qualifications}
                    
                    Preferred Skills:
                    {preferred_skills}
                    """
                    
                    jd = ai_models.generate_text(prompt, max_length=1000)
                    
                    st.subheader("Generated Job Description")
                    st.write(jd)
                    
                    st.download_button(
                        "Download JD",
                        data=jd,
                        file_name=f"JD_{job_title.replace(' ', '_')}.txt",
                        mime="text/plain"
                    )
    
    with tab3:
        st.subheader("Onboarding Toolkit")
        
        st.write("### New Hire Checklist")
        checklist_items = [
            "Complete HR paperwork",
            "Set up email and accounts",
            "Provide equipment",
            "Schedule orientation",
            "Assign mentor/buddy",
            "Plan 30-60-90 day goals",
            "Schedule training sessions"
        ]
        
        for item in checklist_items:
            st.checkbox(item)
        
        st.write("### Onboarding Documents")
        doc_options = [
            "Employee Handbook",
            "Benefits Guide",
            "Company Policies",
            "Team Directory",
            "Project Overview"
        ]
        
        selected_docs = st.multiselect("Select documents to include", doc_options)
        
        if st.button("Generate Onboarding Package"):
            # In a real app, this would compile the selected documents
            st.success("Onboarding package generated!")
            st.download_button(
                "Download Package",
                data="\n".join(selected_docs),
                file_name="Onboarding_Package.zip",
                mime="application/zip"
            )
    
    cur.close()
    conn.close()

# Tax & GST Module
def tax_module(business_id, ai_models):
    st.header("🧾 Automated Tax & GST Filing")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2, tab3 = st.tabs(["Tax Records", "GST Calculator", "Filing Status"])
    
    with tab1:
        st.subheader("Tax Records")
        
        cur.execute("SELECT * FROM tax_records WHERE business_id = %s ORDER BY financial_year DESC", (business_id,))
        records = cur.fetchall()
        
        if records:
            df = pd.DataFrame(records, columns=[
                "ID", "Financial Year", "Total Income", "Tax Paid", 
                "Filing Date", "Status", "Notes", "Business ID"
            ])
            st.dataframe(df.drop(columns=["Business ID"]), hide_index=True)
            
            # Tax summary
            st.write("### Tax Summary")
            total_tax = df["Tax Paid"].sum()
            avg_rate = (total_tax / df["Total Income"].sum()) * 100 if df["Total Income"].sum() > 0 else 0
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Tax Paid", f"${total_tax:,.2f}")
            with col2:
                st.metric("Average Tax Rate", f"{avg_rate:.1f}%")
        else:
            st.info("No tax records found.")
        
        # Add new record
        with st.expander("Add Tax Record"):
            with st.form("add_tax_record"):
                financial_year = st.text_input("Financial Year (e.g., 2023-24)")
                total_income = st.number_input("Total Income", min_value=0.0, step=1000.0)
                tax_paid = st.number_input("Tax Paid", min_value=0.0, step=1000.0)
                filing_date = st.date_input("Filing Date")
                status = st.selectbox("Status", ["Filed", "Pending", "Revised", "Extension"])
                notes = st.text_area("Notes")
                
                if st.form_submit_button("Add Record"):
                    cur.execute(
                        """INSERT INTO tax_records 
                        (business_id, financial_year, total_income, tax_paid, filing_date, status, notes) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, financial_year, total_income, tax_paid, filing_date, status, notes)
                    )
                    conn.commit()
                    st.success("Tax record added successfully!")
                    st.rerun()
    
    with tab2:
        st.subheader("GST Calculator")
        
        col1, col2 = st.columns(2)
        with col1:
            amount = st.number_input("Amount", min_value=0.0, step=100.0)
        with col2:
            gst_rate = st.selectbox("GST Rate", ["5%", "12%", "18%", "28%"])
        
        if amount > 0:
            rate = float(gst_rate.replace("%", "")) / 100
            gst_amount = amount * rate
            total = amount + gst_amount
            
            st.write("### Calculation Results")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Base Amount", f"₹{amount:,.2f}")
            with col2:
                st.metric(f"GST {gst_rate}", f"₹{gst_amount:,.2f}")
            with col3:
                st.metric("Total", f"₹{total:,.2f}")
            
            # GST filing due dates
            st.write("### Upcoming GST Filing Dates")
            today = datetime.now().date()
            next_month = today.replace(day=1) + timedelta(days=32)
            due_date = next_month.replace(day=20)
            
            st.write(f"- GSTR-3B for {next_month.strftime('%B %Y')}: **{due_date.strftime('%d %B %Y')}**")
            st.warning(f"⏰ {(due_date - today).days} days remaining")
    
    with tab3:
        st.subheader("Filing Status")
        
        # Simulated filing tracker
        filings = [
            {
                "form": "GSTR-1",
                "period": "July 2023",
                "status": "Filed",
                "date": (datetime.now() - timedelta(days=10)).date(),
                "due_date": (datetime.now() - timedelta(days=5)).date()
            },
            {
                "form": "GSTR-3B",
                "period": "July 2023",
                "status": "Filed",
                "date": (datetime.now() - timedelta(days=5)).date(),
                "due_date": (datetime.now() - timedelta(days=2)).date()
            },
            {
                "form": "GSTR-1",
                "period": "August 2023",
                "status": "Pending",
                "date": None,
                "due_date": (datetime.now() + timedelta(days=5)).date()
            }
        ]
        
        for filing in filings:
            with st.expander(f"{filing['form']} - {filing['period']}"):
                st.write(f"**Status:** {filing['status']}")
                if filing['date']:
                    st.write(f"**Filed On:** {filing['date']}")
                st.write(f"**Due Date:** {filing['due_date']}")
                
                if filing['status'] == "Pending":
                    days_left = (filing['due_date'] - datetime.now().date()).days
                    if days_left > 0:
                        st.warning(f"⏰ {days_left} days remaining")
                    else:
                        st.error("❌ Overdue")
                
                if st.button("File Now", key=f"file_{filing['form']}_{filing['period']}"):
                    st.info("In a real app, this would open the GST portal")
    
    cur.close()
    conn.close()

# IPO & Cap Table Module
def ipo_module(business_id, ai_models):
    st.header("📊 IPO & Cap Table Management")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2, tab3 = st.tabs(["IPO Tracker", "Cap Table", "Investor Relations"])
    
    with tab1:
        st.subheader("IPO Tracker")
        
        cur.execute("SELECT * FROM ipo_data WHERE business_id = %s", (business_id,))
        ipos = cur.fetchall()
        
        if ipos:
            df = pd.DataFrame(ipos, columns=[
                "ID", "Company", "Issue Size", "Price Range", 
                "Open Date", "Close Date", "Status", "Allotment Date", "Listing Date", "Business ID"
            ])
            st.dataframe(df.drop(columns=["Business ID"]), hide_index=True)
            
            # Filter by status
            status_filter = st.multiselect(
                "Filter by Status",
                options=df["Status"].unique(),
                default=df["Status"].unique()
            )
            
            if status_filter:
                filtered_df = df[df["Status"].isin(status_filter)]
                st.dataframe(filtered_df.drop(columns=["Business ID"]), hide_index=True)
        else:
            st.info("No IPO data available.")
        
        # Add new IPO
        with st.expander("Add IPO Details"):
            with st.form("add_ipo"):
                company_name = st.text_input("Company Name")
                issue_size = st.number_input("Issue Size (₹)", min_value=0.0, step=1000000.0)
                price_range = st.text_input("Price Range (₹)")
                open_date = st.date_input("Open Date")
                close_date = st.date_input("Close Date")
                status = st.selectbox("Status", [
                    "Upcoming", "Open", "Closed", "Allotted", "Listed"
                ])
                allotment_date = st.date_input("Allotment Date")
                listing_date = st.date_input("Listing Date")
                
                if st.form_submit_button("Add IPO"):
                    cur.execute(
                        """INSERT INTO ipo_data 
                        (business_id, company_name, issue_size, price_range, open_date, close_date, status, allotment_date, listing_date) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, company_name, issue_size, price_range, open_date, close_date, status, allotment_date, listing_date)
                    )
                    conn.commit()
                    st.success("IPO details added successfully!")
                    st.rerun()
    
    with tab2:
        st.subheader("Cap Table Management")
        
        # Simulated cap table
        stakeholders = [
            {"name": "Founders", "shares": 5000000, "percentage": 50.0, "type": "Common"},
            {"name": "Seed Investors", "shares": 2000000, "percentage": 20.0, "type": "Preferred"},
            {"name": "Series A Investors", "shares": 2000000, "percentage": 20.0, "type": "Preferred"},
            {"name": "Employee Pool", "shares": 1000000, "percentage": 10.0, "type": "Options"}
        ]
        
        df_cap = pd.DataFrame(stakeholders)
        
        # Visualization
        fig = px.pie(
            df_cap, 
            values="percentage", 
            names="name", 
            title="Cap Table Ownership"
        )
        st.plotly_chart(fig)
        
        # Detailed view
        st.write("### Detailed Cap Table")
        st.dataframe(df_cap)
        
        # Waterfall analysis
        st.write("### Waterfall Analysis (Pre-IPO)")
        waterfall_data = [
            {"stage": "Pre-Seed", "valuation": 5000000},
            {"stage": "Seed", "valuation": 20000000},
            {"stage": "Series A", "valuation": 50000000},
            {"stage": "Series B", "valuation": 120000000},
            {"stage": "IPO Projection", "valuation": 500000000}
        ]
        
        fig = px.funnel(
            pd.DataFrame(waterfall_data), 
            x="valuation", 
            y="stage", 
            title="Valuation Growth"
        )
        st.plotly_chart(fig)
    
    with tab3:
        st.subheader("Investor Relations")
        
        # Simulated investor communications
        communications = [
            {
                "date": (datetime.now() - timedelta(days=30)).date(),
                "type": "Quarterly Report",
                "recipients": "All Investors",
                "status": "Sent"
            },
            {
                "date": (datetime.now() - timedelta(days=15)).date(),
                "type": "Board Meeting",
                "recipients": "Board Members",
                "status": "Completed"
            },
            {
                "date": (datetime.now() + timedelta(days=10)).date(),
                "type": "Roadshow",
                "recipients": "Institutional Investors",
                "status": "Scheduled"
            }
        ]
        
        st.write("### Recent Communications")
        for comm in communications:
            with st.expander(f"{comm['date']}: {comm['type']}"):
                st.write(f"**Recipients:** {comm['recipients']}")
                st.write(f"**Status:** {comm['status']}")
                
                if comm['status'] == "Scheduled":
                    days_left = (comm['date'] - datetime.now().date()).days
                    st.warning(f"⏰ {days_left} days remaining")
        
        # New communication
        with st.expander("Schedule New Communication"):
            with st.form("new_communication"):
                comm_type = st.selectbox("Type", [
                    "Investor Update", "Board Meeting", "Roadshow", "Earnings Call"
                ])
                recipients = st.text_input("Recipients")
                scheduled_date = st.date_input("Date")
                notes = st.text_area("Notes")
                
                if st.form_submit_button("Schedule"):
                    # In a real app, this would save to database
                    st.success("Communication scheduled successfully!")
    
    cur.close()
    conn.close()

# Legal Marketplace Module
def legal_marketplace(business_id, ai_models):
    st.header("⚖️ Legal, CA & Insurance Marketplace")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    tab1, tab2, tab3 = st.tabs(["Legal", "Chartered Accountants", "Insurance"])
    
    with tab1:
        st.subheader("Legal Service Providers")
        
        cur.execute("SELECT * FROM service_providers WHERE business_id = %s AND service_type = 'legal'", (business_id,))
        providers = cur.fetchall()
        
        if providers:
            df = pd.DataFrame(providers, columns=[
                "ID", "Name", "Service Type", "Email", 
                "Rating", "Experience", "Pricing", "Available", "Business ID"
            ])
            st.dataframe(df.drop(columns=["Business ID", "Service Type"]), hide_index=True)
        else:
            st.info("No legal service providers registered yet.")
        
        # Add new legal provider
        with st.expander("Add Legal Provider"):
            with st.form("add_legal_provider"):
                name = st.text_input("Provider Name")
                email = st.text_input("Contact Email")
                rating = st.slider("Rating", 1.0, 5.0, 4.0, step=0.1)
                experience = st.number_input("Years of Experience", min_value=1, step=1)
                pricing = st.text_input("Pricing")
                available = st.checkbox("Currently Available", value=True)
                
                if st.form_submit_button("Add Provider"):
                    cur.execute(
                        """INSERT INTO service_providers 
                        (business_id, name, service_type, contact_email, rating, experience_years, pricing, availability) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, name, "legal", email, rating, experience, pricing, available)
                    )
                    conn.commit()
                    st.success("Legal provider added successfully!")
                    st.rerun()
    
    with tab2:
        st.subheader("Chartered Accountants")
        
        cur.execute("SELECT * FROM service_providers WHERE business_id = %s AND service_type = 'ca'", (business_id,))
        providers = cur.fetchall()
        
        if providers:
            df = pd.DataFrame(providers, columns=[
                "ID", "Name", "Service Type", "Email", 
                "Rating", "Experience", "Pricing", "Available", "Business ID"
            ])
            st.dataframe(df.drop(columns=["Business ID", "Service Type"]), hide_index=True)
        else:
            st.info("No CA service providers registered yet.")
        
        # Add new CA
        with st.expander("Add CA"):
            with st.form("add_ca"):
                name = st.text_input("CA Name")
                email = st.text_input("Contact Email")
                rating = st.slider("Rating", 1.0, 5.0, 4.0, step=0.1)
                experience = st.number_input("Years of Experience", min_value=1, step=1)
                pricing = st.text_input("Pricing")
                available = st.checkbox("Currently Available", value=True)
                
                if st.form_submit_button("Add CA"):
                    cur.execute(
                        """INSERT INTO service_providers 
                        (business_id, name, service_type, contact_email, rating, experience_years, pricing, availability) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, name, "ca", email, rating, experience, pricing, available)
                    )
                    conn.commit()
                    st.success("CA added successfully!")
                    st.rerun()
    
    with tab3:
        st.subheader("Insurance Providers")
        
        cur.execute("SELECT * FROM service_providers WHERE business_id = %s AND service_type = 'insurance'", (business_id,))
        providers = cur.fetchall()
        
        if providers:
            df = pd.DataFrame(providers, columns=[
                "ID", "Name", "Service Type", "Email", 
                "Rating", "Experience", "Pricing", "Available", "Business ID"
            ])
            st.dataframe(df.drop(columns=["Business ID", "Service Type"]), hide_index=True)
        else:
            st.info("No insurance providers registered yet.")
        
        # Add new insurance provider
        with st.expander("Add Insurance Provider"):
            with st.form("add_insurance"):
                name = st.text_input("Provider Name")
                email = st.text_input("Contact Email")
                rating = st.slider("Rating", 1.0, 5.0, 4.0, step=0.1)
                experience = st.number_input("Years of Experience", min_value=1, step=1)
                pricing = st.text_input("Pricing")
                available = st.checkbox("Currently Available", value=True)
                
                if st.form_submit_button("Add Provider"):
                    cur.execute(
                        """INSERT INTO service_providers 
                        (business_id, name, service_type, contact_email, rating, experience_years, pricing, availability) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (business_id, name, "insurance", email, rating, experience, pricing, available)
                    )
                    conn.commit()
                    st.success("Insurance provider added successfully!")
                    st.rerun()
    
    # Provider search across all categories
    st.write("### Find Service Provider")
    service_type = st.selectbox("Service Needed", [
        "Legal", "Accounting", "Insurance", "Consulting"
    ])
    min_rating = st.slider("Minimum Rating", 1.0, 5.0, 4.0, step=0.1)
    max_price = st.text_input("Maximum Budget (leave blank for any)")
    
    if st.button("Search"):
        # In a real app, this would query the database with filters
        st.info(f"Showing {service_type} providers with rating ≥ {min_rating}")
        
        # Simulated results
        results = [
            {
                "name": f"{service_type} Professionals",
                "rating": 4.5,
                "experience": 10,
                "pricing": "$150/hour" if service_type == "Legal" else "₹8,000/month"
            },
            {
                "name": f"{service_type} Solutions",
                "rating": 4.2,
                "experience": 7,
                "pricing": "$200/hour" if service_type == "Legal" else "₹10,000/month"
            }
        ]
        
        for result in results:
            with st.expander(f"{result['name']} ({result['rating']}★)"):
                st.write(f"**Experience:** {result['experience']} years")
                st.write(f"**Pricing:** {result['pricing']}")
                if st.button("Contact", key=f"contact_{result['name']}"):
                    st.success("Contact information would appear here")
    
    cur.close()
    conn.close()
def get_financial_performance_data(business_id, num_periods=12, period_type='M'):
    """
    Fetches financial performance data (Revenue, Expenses, Profit) for the last num_periods.
    period_type can be 'M' for Monthly or 'Q' for Quarterly.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    end_date = datetime.now().date()
    
    if period_type == 'M':
        dates = pd.date_range(end=end_date.replace(day=1), periods=num_periods, freq='MS')
        period_label_format = '%Y-%m'
        sql_trunc_period = 'month'
    elif period_type == 'Q':
        current_quarter_start = pd.Timestamp(end_date).to_period('Q').start_time.date()
        dates = pd.date_range(end=current_quarter_start, periods=num_periods, freq='QS')
        sql_trunc_period = 'quarter'
    else: # Default to monthly
        dates = pd.date_range(end=end_date.replace(day=1), periods=num_periods, freq='MS')
        period_label_format = '%Y-%m'
        sql_trunc_period = 'month'

    df_data = pd.DataFrame({'PeriodStart': dates})
    
    if period_type == 'Q':
        df_data['PeriodLabel'] = df_data['PeriodStart'].apply(lambda x: f"{x.year}-Q{((x.month-1)//3)+1}")
    else:
        df_data['PeriodLabel'] = df_data['PeriodStart'].dt.strftime(period_label_format)

    # Fetch Revenue
    try:
        query_revenue = sql.SQL("""
            SELECT DATE_TRUNC(%s, issue_date) as period_start_db, SUM(total_amount) as revenue
            FROM invoices
            WHERE business_id = %s AND issue_date >= %s AND issue_date <= %s
            GROUP BY period_start_db
            ORDER BY period_start_db;
        """)
        min_date_for_query = dates.min().date() if hasattr(dates.min(), 'date') else dates.min()
        cur.execute(query_revenue, (sql_trunc_period, business_id, min_date_for_query, end_date))
        revenue_data = cur.fetchall()
        df_revenue = pd.DataFrame(revenue_data, columns=['PeriodStart', 'Revenue'])
        if not df_revenue.empty:
            df_revenue['PeriodStart'] = pd.to_datetime(df_revenue['PeriodStart'])
            df_data = pd.merge(df_data, df_revenue, on='PeriodStart', how='left')
        else:
            df_data['Revenue'] = 0.0
    except Exception as e:
        st.error(f"Error fetching revenue data: {e}")
        df_data['Revenue'] = 0.0
    df_data['Revenue'] = df_data['Revenue'].fillna(0.0).astype(float)

    # Fetch Expenses (Simplified: using total monthly salaries)
    try:
        cur.execute("SELECT SUM(salary) FROM employees WHERE business_id = %s", (business_id,))
        total_monthly_salary_tuple = cur.fetchone()
        total_monthly_salary = float(total_monthly_salary_tuple[0]) if total_monthly_salary_tuple and total_monthly_salary_tuple[0] is not None else 0.0
        
        expense_per_period = total_monthly_salary * 3 if period_type == 'Q' else total_monthly_salary
        df_data['Expenses'] = expense_per_period
    except Exception as e:
        st.error(f"Error fetching salary data for expenses: {e}")
        df_data['Expenses'] = 0.0
    df_data['Expenses'] = df_data['Expenses'].fillna(0.0).astype(float)
    
    df_data['Profit'] = df_data['Revenue'] - df_data['Expenses']
    
    cur.close()
    conn.close()
    return df_data[['PeriodLabel', 'Revenue', 'Expenses', 'Profit']].rename(columns={'PeriodLabel': 'Period'})

# --- Inventory Turnover Data (from previous response, assumed to be good) ---
def get_inventory_turnover_data(business_id, lookback_days=365):
    conn = get_db_connection()
    cur = conn.cursor()
    cogs = 0.0
    current_inventory_value_at_cost = 0.0
    end_date_dt = datetime.now().date()
    start_date_cogs_dt = end_date_dt - timedelta(days=lookback_days)

    try:
        cur.execute("SELECT id, price, quantity FROM products WHERE business_id = %s", (business_id,))
        products_details = cur.fetchall()
        product_info = {pid: {'price': float(p) if p else 0.0, 'quantity': int(q) if q else 0} for pid, p, q in products_details}

        cur.execute(
            "SELECT items FROM invoices WHERE business_id = %s AND issue_date BETWEEN %s AND %s",
            (business_id, start_date_cogs_dt, end_date_dt)
        )
        invoices_items_json = cur.fetchall()
        
        for items_json_tuple in invoices_items_json:
            if items_json_tuple and items_json_tuple[0]:
                items_list = json.loads(items_json_tuple[0]) if isinstance(items_json_tuple[0], str) else items_json_tuple[0]
                for item_detail in items_list:
                    product_id = item_detail.get('product_id')
                    quantity_sold = item_detail.get('quantity', 0)
                    if product_id in product_info:
                        selling_price = product_info[product_id]['price']
                        cost_price_estimate = selling_price * 0.6 # Cost margin assumption
                        cogs += quantity_sold * cost_price_estimate

        for info in product_info.values():
            cost_price_estimate = info['price'] * 0.6
            current_inventory_value_at_cost += info['quantity'] * cost_price_estimate
                
        inventory_turnover_raw = cogs / current_inventory_value_at_cost if current_inventory_value_at_cost > 0 else 0.0
        target_turnover, trend = 6.0, "neutral"
        if inventory_turnover_raw > target_turnover * 0.8: trend = "up"
        elif inventory_turnover_raw < target_turnover * 0.5: trend = "critical_low"
        else: trend = "down"

    except Exception as e:
        st.error(f"Error calculating inventory turnover: {e}")
        inventory_turnover_raw, target_turnover, trend = 0.0, 6.0, "error"
    finally:
        cur.close()
        conn.close()
        
    return {"name": "Inventory Turnover", "value": round(inventory_turnover_raw, 1), "target": target_turnover, "trend": trend}

# --- Sales Performance Report Data (from previous response, assumed to be good) ---
def get_sales_performance_report_data(business_id, start_date, end_date):
    conn = get_db_connection()
    cur = conn.cursor()
    report = {"total_revenue": 0.0, "top_product_name": "N/A", "top_product_revenue": 0.0, "num_invoices": 0, "avg_invoice_value": 0.0}
    try:
        cur.execute(
            "SELECT SUM(total_amount), COUNT(id) FROM invoices WHERE business_id = %s AND issue_date BETWEEN %s AND %s",
            (business_id, start_date, end_date)
        )
        result = cur.fetchone()
        if result:
            report["total_revenue"] = float(result[0] or 0.0)
            report["num_invoices"] = int(result[1] or 0)
        if report["num_invoices"] > 0:
            report["avg_invoice_value"] = report["total_revenue"] / report["num_invoices"]

        cur.execute(
            "SELECT items FROM invoices WHERE business_id = %s AND issue_date BETWEEN %s AND %s",
            (business_id, start_date, end_date)
        )
        all_items_json = cur.fetchall()
        product_sales = {}
        for items_json_tuple in all_items_json:
            if items_json_tuple and items_json_tuple[0]:
                items_list = json.loads(items_json_tuple[0]) if isinstance(items_json_tuple[0], str) else items_json_tuple[0]
                for item in items_list:
                    name, total = item.get("name"), item.get("total", 0.0)
                    if name: product_sales[name] = product_sales.get(name, 0.0) + float(total)
        
        if product_sales:
            top_product = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[0]
            report["top_product_name"], report["top_product_revenue"] = top_product[0], top_product[1]
    except Exception as e:
        st.error(f"Error generating sales report data: {e}")
    finally:
        cur.close()
        conn.close()
    return report

# --- Helper for Deltas ---
def calculate_delta(current, previous):
    if previous == 0: return "N/A" if current == 0 else "New"
    return f"{(current - previous) / previous * 100:.1f}%"

# --- NEW: Customer Lifetime Value (CLV) ---
def get_customer_lifetime_value_data(business_id):
    conn = get_db_connection()
    cur = conn.cursor()
    clv_avg = 0.0
    num_customers = 0
    try:
        # Using customer_email to identify unique customers
        cur.execute(
            """
            SELECT customer_email, SUM(total_amount) as total_spent
            FROM invoices
            WHERE business_id = %s AND customer_email IS NOT NULL AND customer_email != ''
            GROUP BY customer_email;
            """, (business_id,)
        )
        customer_spending = cur.fetchall()
        if customer_spending:
            df_spending = pd.DataFrame(customer_spending, columns=['customer_email', 'total_spent'])
            df_spending['total_spent'] = df_spending['total_spent'].astype(float)
            clv_avg = df_spending['total_spent'].mean()
            num_customers = df_spending['customer_email'].nunique()
    except Exception as e:
        st.error(f"Error calculating CLV: {e}")
    finally:
        cur.close()
        conn.close()
    
    # Target CLV can be a business goal, placeholder for now
    target_clv = 1000 
    trend = "up" if clv_avg > target_clv * 0.8 else ("down" if clv_avg < target_clv else "neutral")

    return {
        "name": "Avg. Customer Lifetime Value (CLV)",
        "value": round(clv_avg, 0),
        "target": target_clv,
        "trend": trend,
        "unit": "$",
        "note": f"Based on {num_customers} unique customers." if num_customers > 0 else "No customer data for CLV."
    }

# --- NEW: Employee Productivity Score (Average) ---
def get_employee_productivity_score_data(business_id):
    conn = get_db_connection()
    cur = conn.cursor()
    avg_score = 0.0
    num_employees_rated = 0
    try:
        cur.execute(
            "SELECT AVG(performance_score), COUNT(id) FROM employees WHERE business_id = %s AND performance_score IS NOT NULL",
            (business_id,)
        )
        result = cur.fetchone()
        if result and result[0] is not None:
            avg_score = float(result[0])
            num_employees_rated = int(result[1])
    except Exception as e:
        st.error(f"Error fetching employee productivity score: {e}")
    finally:
        cur.close()
        conn.close()

    target_score = 8.0 # Assuming score is out of 10
    trend = "up" if avg_score >= target_score * 0.9 else ("down" if avg_score < target_score * 0.7 else "neutral")
    
    return {
        "name": "Avg. Employee Productivity Score",
        "value": round(avg_score, 1),
        "target": target_score,
        "trend": trend,
        "unit": "/10",
        "note": f"Based on {num_employees_rated} rated employees." if num_employees_rated > 0 else "No rated employees."
    }

# --- NEW: Operational Efficiency Trends (Project Progress) ---
def get_operational_efficiency_trends_data(business_id, num_months=6):
    conn = get_db_connection()
    cur = conn.cursor()
    df_efficiency = pd.DataFrame(columns=["Month", "Efficiency"])
    try:
        # Consider projects that were active or completed in the last num_months
        # This is a simplified view of "efficiency"
        end_date = datetime.now().date()
        start_date_lookback = (end_date.replace(day=1) - timedelta(days=1)).replace(day=1) # Start of prev month
        for _ in range(num_months -1):
            start_date_lookback = (start_date_lookback - timedelta(days=1)).replace(day=1)

        query = sql.SQL("""
            SELECT DATE_TRUNC('month', COALESCE(end_date, start_date)) as project_month, AVG(progress) as avg_progress
            FROM projects
            WHERE business_id = %s AND (
                  (end_date >= %s AND end_date <= %s) OR 
                  (start_date <= %s AND (end_date IS NULL OR end_date >= %s)) 
            )
            GROUP BY project_month
            ORDER BY project_month DESC
            LIMIT %s;
        """)
        cur.execute(query, (business_id, start_date_lookback, end_date, end_date, start_date_lookback, num_months))
        data = cur.fetchall()
        if data:
            df_efficiency = pd.DataFrame(data, columns=["Month", "Efficiency"])
            df_efficiency["Month"] = pd.to_datetime(df_efficiency["Month"]).dt.strftime('%Y-%m')
            df_efficiency["Efficiency"] = df_efficiency["Efficiency"].astype(float).round(1)
            df_efficiency = df_efficiency.sort_values(by="Month").reset_index(drop=True)

    except Exception as e:
        st.error(f"Error fetching operational efficiency trends: {e}")
    finally:
        cur.close()
        conn.close()
    return df_efficiency

# --- NEW: Employee Productivity Report Data ---
def get_employee_productivity_report_data(business_id, start_date_filter, end_date_filter):
    # Note: start_date_filter and end_date_filter are for report period,
    # but employee data is mostly current. We can use last_appraisal_date for filtering if needed.
    conn = get_db_connection()
    cur = conn.cursor()
    report = {
        "avg_score_overall": 0.0,
        "top_performer_name": "N/A",
        "top_performer_score": 0.0,
        "department_scores": [], # List of dicts: {"department": "X", "avg_score": Y}
        "num_employees_total": 0,
        "num_employees_rated": 0,
    }
    try:
        cur.execute(
            "SELECT name, department, performance_score, salary FROM employees WHERE business_id = %s",
            (business_id,)
        )
        employees_data = cur.fetchall()
        report["num_employees_total"] = len(employees_data)

        if employees_data:
            df_employees = pd.DataFrame(employees_data, columns=["name", "department", "performance_score", "salary"])
            df_employees["performance_score"] = pd.to_numeric(df_employees["performance_score"], errors='coerce')
            
            rated_employees = df_employees.dropna(subset=['performance_score'])
            report["num_employees_rated"] = len(rated_employees)

            if not rated_employees.empty:
                report["avg_score_overall"] = round(rated_employees["performance_score"].mean(), 1)
                
                top_perf = rated_employees.loc[rated_employees["performance_score"].idxmax()]
                report["top_performer_name"] = top_perf["name"]
                report["top_performer_score"] = top_perf["performance_score"]
                
                dept_scores_series = rated_employees.groupby("department")["performance_score"].mean().round(1)
                report["department_scores"] = [{"department": idx, "avg_score": val} for idx, val in dept_scores_series.items()]
    except Exception as e:
        st.error(f"Error generating employee productivity report: {e}")
    finally:
        cur.close()
        conn.close()
    return report

# --- NEW: Inventory Analysis Report Data ---
def get_inventory_analysis_report_data(business_id, start_date_filter, end_date_filter):
    conn = get_db_connection()
    cur = conn.cursor()
    report = {
        "total_inventory_value_at_price": 0.0,
        "total_inventory_value_at_cost_est": 0.0,
        "num_products": 0,
        "slow_moving_items_count": 0,
        "top_slow_moving_items": [], # List of dicts {"name": X, "quantity": Y, "days_since_last_sale_est": Z}
        "inventory_turnover": get_inventory_turnover_data(business_id)['value'] # Reuse existing
    }
    cost_margin = 0.6 # Assumption

    try:
        cur.execute(
            "SELECT id, name, price, quantity FROM products WHERE business_id = %s",
            (business_id,)
        )
        products_data = cur.fetchall()
        report["num_products"] = len(products_data)

        if products_data:
            df_products = pd.DataFrame(products_data, columns=["id", "name", "price", "quantity"])
            df_products["price"] = pd.to_numeric(df_products["price"], errors='coerce').fillna(0)
            df_products["quantity"] = pd.to_numeric(df_products["quantity"], errors='coerce').fillna(0)
            
            df_products["value_at_price"] = df_products["price"] * df_products["quantity"]
            df_products["value_at_cost_est"] = df_products["value_at_price"] * cost_margin
            
            report["total_inventory_value_at_price"] = df_products["value_at_price"].sum()
            report["total_inventory_value_at_cost_est"] = df_products["value_at_cost_est"].sum()

            # Identify slow-moving items (e.g., not sold in last 90 days and high quantity)
            # This requires sales data.
            cur.execute(
                """SELECT items, issue_date FROM invoices 
                   WHERE business_id = %s AND issue_date BETWEEN %s AND %s""",
                (business_id, start_date_filter, end_date_filter) # Use report period for sales
            )
            invoices_items_json = cur.fetchall()
            
            product_sales_dates = {} # {product_id: last_sale_date}
            for items_json_tuple, issue_date_val in invoices_items_json:
                if items_json_tuple and items_json_tuple[0]:
                    items_list = json.loads(items_json_tuple[0]) if isinstance(items_json_tuple[0], str) else items_json_tuple[0]
                    for item_detail in items_list:
                        product_id = item_detail.get('product_id')
                        if product_id:
                            current_last_sale = product_sales_dates.get(product_id)
                            if current_last_sale is None or issue_date_val > current_last_sale:
                                product_sales_dates[product_id] = issue_date_val
            
            today = datetime.now().date()
            slow_threshold_days = 90 # Define what "slow" means
            
            slow_items_list = []
            for index, row in df_products.iterrows():
                last_sale_date = product_sales_dates.get(row['id'])
                days_since_sale = (today - last_sale_date).days if last_sale_date else float('inf')
                
                # Criteria for slow-moving: high quantity and not sold recently (or ever in period)
                # Adjust quantity_threshold as needed, e.g., based on average stock or value
                quantity_threshold = 20 
                if row['quantity'] > quantity_threshold and days_since_sale > slow_threshold_days:
                    slow_items_list.append({
                        "name": row["name"], 
                        "quantity": row["quantity"],
                        "days_since_last_sale_est": days_since_sale if days_since_sale != float('inf') else "Never in period"
                    })
            
            report["slow_moving_items_count"] = len(slow_items_list)
            # Sort by quantity descending for "top" slow-moving
            report["top_slow_moving_items"] = sorted(slow_items_list, key=lambda x: x["quantity"], reverse=True)[:5]

    except Exception as e:
        st.error(f"Error generating inventory analysis report: {e}")
    finally:
        cur.close()
        conn.close()
    return report
# Enterprise Intelligence Module
def enterprise_intelligence(business_id, ai_models): # ai_models might not be used here for now
    st.header("📊 Enterprise Intelligence Dashboards")
    
    tab1, tab2, tab3 = st.tabs([
        "Financial Performance", 
        "Operational Metrics", 
        "Custom Reports"
    ])
    
    with tab1:
        st.subheader("Financial Performance")
        
        fin_period_type = st.selectbox("View Financials By:", ["Monthly", "Quarterly"], key="fin_period_type_ei")
        num_fin_periods = st.slider("Number of Past Periods:", min_value=3, max_value=24, 
                                    value=12 if fin_period_type == "Monthly" else 8, key="num_fin_periods_ei")

        with st.spinner("Fetching financial data..."):
            df_finance = get_financial_performance_data(business_id, num_periods=num_fin_periods, period_type=fin_period_type[0])
        
        if not df_finance.empty:
            fig_finance = px.line(
                df_finance, 
                x="Period", 
                y=["Revenue", "Expenses", "Profit"],
                title=f"{fin_period_type} Financial Performance ({num_fin_periods} Periods)"
            )
            st.plotly_chart(fig_finance, use_container_width=True)
            
            st.write(f"### Key Financial Metrics (Latest {fin_period_type[:-2]})")
            latest_period_data = df_finance.iloc[-1]
            revenue_latest = latest_period_data['Revenue']
            expenses_latest = latest_period_data['Expenses']
            profit_latest = latest_period_data['Profit']
            
            revenue_prev, expenses_prev, profit_prev = 0.0, 0.0, 0.0
            delta_label = f"vs Prev. {fin_period_type[:-2]}"
            if len(df_finance) >= 2:
                prev_period_data = df_finance.iloc[-2]
                revenue_prev = prev_period_data['Revenue']
                expenses_prev = prev_period_data['Expenses']
                profit_prev = prev_period_data['Profit']
            else:
                delta_label = "(No Previous Period Data)"

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(f"Total Revenue", f"${revenue_latest:,.0f}", 
                         f"{calculate_delta(revenue_latest, revenue_prev)} {delta_label}")
            with col2:
                st.metric(f"Total Expenses (Est.)", f"${expenses_latest:,.0f}", 
                         f"{calculate_delta(expenses_latest, expenses_prev)} {delta_label}")
                st.caption("Expenses estimated based on total salaries.")
            with col3:
                st.metric(f"Net Profit (Est.)", f"${profit_latest:,.0f}", 
                         f"{calculate_delta(profit_latest, profit_prev)} {delta_label}")
        else:
            st.info("No financial data available to display for the selected period.")
    
    with tab2:
        st.subheader("Operational Metrics")
        
        operational_metrics_data = []
        with st.spinner("Fetching operational metrics..."):
            operational_metrics_data.append(get_inventory_turnover_data(business_id))
            operational_metrics_data.append(get_customer_lifetime_value_data(business_id))
            operational_metrics_data.append(get_employee_productivity_score_data(business_id))

            # Metrics that require more data/setup
            operational_metrics_data.append({"name": "Customer Acquisition Cost (CAC)", "value": "N/A", "target": 120, "trend": "neutral", "unit": "$", "note": "Requires marketing spend data."})
            operational_metrics_data.append({"name": "Conversion Rate", "value": "N/A", "target": 4.0, "trend": "neutral", "unit": "%", "note": "Requires lead tracking data."})
            operational_metrics_data.append({"name": "Churn Rate", "value": "N/A", "target": 4.0, "trend": "neutral", "unit": "%", "note": "Requires customer lifecycle data."})

        cols_op = st.columns(3)
        for i, metric in enumerate(operational_metrics_data):
            with cols_op[i % 3]:
                delta_display = ""
                if isinstance(metric.get('value'), (int, float)) and isinstance(metric.get('target'), (int, float)):
                    delta_val = metric['value'] - metric['target']
                    delta_display = f"{delta_val:+.1f} vs target"
                elif metric.get('trend') and metric['trend'] != "neutral":
                     delta_display = f"Trend: {metric['trend'].capitalize()}"

                value_display = f"{metric.get('unit', '')}{metric['value']}" if metric.get('unit') == "$" else f"{metric['value']}{metric.get('unit', '')}"
                if metric['value'] == "N/A": value_display = "N/A"


                delta_color_logic = "normal"
                if metric.get('value') != "N/A" and metric.get('target') is not None:
                    is_better = False
                    if metric['name'] in ["Churn Rate", "Customer Acquisition Cost (CAC)"]: # Lower is better
                        is_better = metric['value'] <= metric['target']
                    else: # Higher is better
                        is_better = metric['value'] >= metric['target']
                    delta_color_logic = "normal" if is_better else "inverse"
                
                st.metric(
                    metric["name"],
                    value_display,
                    delta_display,
                    delta_color=delta_color_logic
                )
                if metric.get("note"):
                    st.caption(metric["note"])
        
        st.write("### Operational Efficiency Trends (Project Progress)")
        num_efficiency_months = st.slider("Months for Efficiency Trend:", min_value=3, max_value=12, value=6, key="num_efficiency_months")
        with st.spinner("Fetching efficiency trends..."):
            df_efficiency = get_operational_efficiency_trends_data(business_id, num_months=num_efficiency_months)
        
        if not df_efficiency.empty:
            fig_efficiency = px.line(
                df_efficiency, 
                x="Month", 
                y="Efficiency",
                title=f"Avg. Project Progress (%) Over Last {num_efficiency_months} Months",
                markers=True
            )
            fig_efficiency.update_yaxes(range=[0, 100])
            st.plotly_chart(fig_efficiency, use_container_width=True)
        else:
            st.info("No project data available for efficiency trends.")
    
    with tab3:
        st.subheader("Custom Reports")
        
        report_type = st.selectbox("Select Report Type", [
            "Sales Performance", 
            "Employee Productivity", 
            "Inventory Analysis",
            "Marketing ROI (Simulated - Requires Data)" 
        ], key="custom_report_type_ei")
        
        # Common time period selection for reports
        st.write("#### Select Report Period")
        report_time_period = st.selectbox("Time Period", [
            "Last 7 Days", "Last 30 Days", "Last 90 Days",
            "Last Month", "Last Quarter", "Last Year", "Custom Range"
        ], key="report_time_period_ei")
        
        report_start_date, report_end_date = datetime.now().date(), datetime.now().date()

        if report_time_period == "Custom Range":
            col_sd, col_ed = st.columns(2)
            with col_sd:
                report_start_date = st.date_input("Start Date", datetime.now().date() - timedelta(days=30), key="report_sd_ei")
            with col_ed:
                report_end_date = st.date_input("End Date", datetime.now().date(), key="report_ed_ei")
        else:
            today = datetime.now().date()
            if report_time_period == "Last 7 Days": report_start_date = today - timedelta(days=6)
            elif report_time_period == "Last 30 Days": report_start_date = today - timedelta(days=29)
            elif report_time_period == "Last 90 Days": report_start_date = today - timedelta(days=89)
            elif report_time_period == "Last Month":
                end_of_last_month = today.replace(day=1) - timedelta(days=1)
                report_start_date, report_end_date = end_of_last_month.replace(day=1), end_of_last_month
            elif report_time_period == "Last Quarter":
                current_q_start = pd.Timestamp(today).to_period('Q').start_time.date()
                report_end_date = current_q_start - timedelta(days=1)
                report_start_date = pd.Timestamp(report_end_date).to_period('Q').start_time.date()
            elif report_time_period == "Last Year":
                report_start_date = today.replace(year=today.year - 1, month=1, day=1)
                report_end_date = today.replace(year=today.year - 1, month=12, day=31)
            
            if report_time_period not in ["Last Month", "Last Quarter", "Last Year", "Custom Range"]:
                 report_end_date = today
        
        st.info(f"Report period: {report_start_date.strftime('%Y-%m-%d')} to {report_end_date.strftime('%Y-%m-%d')}")

        if st.button("Generate Report", key="generate_custom_report_ei"):
            with st.spinner(f"Generating {report_type} report..."):
                report_content_for_download = f"Report Type: {report_type}\nPeriod: {report_start_date} to {report_end_date}\n\n"
                file_name_prefix = report_type.replace(" ", "_").split("(")[0].strip()

                if report_type == "Sales Performance":
                    sales_data = get_sales_performance_report_data(business_id, report_start_date, report_end_date)
                    st.success("Sales Performance Report Generated!")
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Revenue", f"${sales_data['total_revenue']:,.2f}")
                    col2.metric("Number of Invoices", f"{sales_data['num_invoices']}")
                    col3.metric("Avg. Invoice Value", f"${sales_data['avg_invoice_value']:,.2f}")
                    st.write(f"**Top Product:** {sales_data['top_product_name']} (Revenue: ${sales_data['top_product_revenue']:,.2f})")
                    
                    report_content_for_download += (
                        f"Total Revenue: ${sales_data['total_revenue']:,.2f}\n"
                        f"Number of Invoices: {sales_data['num_invoices']}\n"
                        f"Average Invoice Value: ${sales_data['avg_invoice_value']:,.2f}\n"
                        f"Top Product: {sales_data['top_product_name']} (Revenue: ${sales_data['top_product_revenue']:,.2f})\n"
                    )

                elif report_type == "Employee Productivity":
                    emp_prod_data = get_employee_productivity_report_data(business_id, report_start_date, report_end_date)
                    st.success("Employee Productivity Report Generated!")
                    st.metric("Overall Avg. Performance Score", f"{emp_prod_data['avg_score_overall']}/10", 
                              help=f"Based on {emp_prod_data['num_employees_rated']} of {emp_prod_data['num_employees_total']} employees.")
                    st.write(f"**Top Performer:** {emp_prod_data['top_performer_name']} (Score: {emp_prod_data['top_performer_score']}/10)")
                    
                    report_content_for_download += (
                        f"Overall Average Performance Score: {emp_prod_data['avg_score_overall']}/10\n"
                        f"Total Employees: {emp_prod_data['num_employees_total']}, Rated: {emp_prod_data['num_employees_rated']}\n"
                        f"Top Performer: {emp_prod_data['top_performer_name']} (Score: {emp_prod_data['top_performer_score']}/10)\n\n"
                        "Department Average Scores:\n"
                    )
                    if emp_prod_data['department_scores']:
                        st.write("##### Department Average Scores:")
                        for dept in emp_prod_data['department_scores']:
                            st.write(f"- {dept['department']}: {dept['avg_score']}/10")
                            report_content_for_download += f"- {dept['department']}: {dept['avg_score']}/10\n"
                    else:
                        st.write("No department scores available.")
                        report_content_for_download += "No department scores available.\n"

                elif report_type == "Inventory Analysis":
                    inv_analysis_data = get_inventory_analysis_report_data(business_id, report_start_date, report_end_date)
                    st.success("Inventory Analysis Report Generated!")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Inventory Value (Selling Price)", f"${inv_analysis_data['total_inventory_value_at_price']:,.2f}")
                    col2.metric("Est. Inventory Value (Cost)", f"${inv_analysis_data['total_inventory_value_at_cost_est']:,.2f}", help="Estimated at 60% of selling price.")
                    col3.metric("Inventory Turnover", f"{inv_analysis_data['inventory_turnover']}x")
                    
                    st.metric("Number of Products", inv_analysis_data['num_products'])
                    st.metric("Slow-Moving Items Count", inv_analysis_data['slow_moving_items_count'], help=f"Items with >20 quantity and no sales in the last 90 days within the period {report_start_date} to {report_end_date}.")

                    report_content_for_download += (
                        f"Total Inventory Value (Selling Price): ${inv_analysis_data['total_inventory_value_at_price']:,.2f}\n"
                        f"Estimated Inventory Value (Cost): ${inv_analysis_data['total_inventory_value_at_cost_est']:,.2f}\n"
                        f"Inventory Turnover: {inv_analysis_data['inventory_turnover']}x\n"
                        f"Number of Products: {inv_analysis_data['num_products']}\n"
                        f"Slow-Moving Items Count: {inv_analysis_data['slow_moving_items_count']}\n\n"
                        "Top Slow-Moving Items (Name, Current Quantity, Days Since Last Sale in Period):\n"
                    )
                    if inv_analysis_data['top_slow_moving_items']:
                        st.write("##### Top Slow-Moving Items (Max 5 Shown):")
                        for item in inv_analysis_data['top_slow_moving_items']:
                            st.write(f"- {item['name']} (Qty: {item['quantity']}, Last Sale: {item['days_since_last_sale_est']} days ago)")
                            report_content_for_download += f"- {item['name']} (Qty: {item['quantity']}, Last Sale: {item['days_since_last_sale_est']} days ago)\n"
                    else:
                        st.write("No significant slow-moving items identified based on current criteria.")
                        report_content_for_download += "No significant slow-moving items identified.\n"
                
                elif report_type == "Marketing ROI (Simulated - Requires Data)":
                    st.warning("Marketing ROI calculation requires dedicated marketing spend and campaign data, which is not yet implemented in the database.")
                    st.info("Displaying simulated data as a placeholder:")
                    st.write("### Marketing ROI Report (Simulated)")
                    st.write("- Total Marketing Spend: $25,000")
                    st.write("- Revenue Attributed to Marketing: $125,000")
                    st.write("- ROI: 5.0x")
                    st.write("- Top Performing Channel: Social Media Ads")
                    report_content_for_download += (
                        "Marketing ROI Report (Simulated - Requires Data Integration)\n"
                        "Total Marketing Spend: $25,000\n"
                        "Revenue Attributed to Marketing: $125,000\n"
                        "ROI: 5.0x\n"
                    )
                
                st.download_button(
                    "Download Report (TXT)",
                    data=report_content_for_download,
                    file_name=f"{file_name_prefix}_Report_{report_start_date}_{report_end_date}.txt",
                    mime="text/plain",
                    key=f"download_{file_name_prefix}_ei"
                )

# AI Market Forecasting Module
def market_forecasting(business_id, ai_models):
    st.header("🔮 AI Market Forecasting")
    
    tab1, tab2 = st.tabs(["Trend Analysis", "Predictive Insights"])
    
    with tab1:
        st.subheader("Market Trend Analysis")
        
        industry = st.selectbox("Select Industry for Analysis", [
            "Technology", "Retail", "Healthcare", "Finance", 
            "Manufacturing", "Energy", "Transportation"
        ])
        
        metric = st.selectbox("Select Metric", [
            "Market Size", "Growth Rate", "Adoption Rate", 
            "Investment Activity", "Regulatory Changes"
        ])
        
        if st.button("Analyze Trends"):
            with st.spinner("Analyzing market trends..."):
                time.sleep(3)  # Simulate analysis
                
                # Generate fake trend analysis
                prompt = f"""
                Provide a detailed analysis of {metric} trends in the {industry} industry 
                over the past 5 years and projected for the next 3 years.
                Include key drivers, challenges, and opportunities.
                """
                
                analysis = ai_models.generate_text(prompt, max_length=1500)
                
                st.subheader(f"{industry} Industry {metric} Analysis")
                st.write(analysis)
                
                # Simulated trend chart
                years = list(range(2018, 2026))
                values = np.random.normal(
                    loc=10 if "Rate" in metric else 100, 
                    scale=3 if "Rate" in metric else 30, 
                    size=len(years)
                ).cumsum()
                
                df_trend = pd.DataFrame({"Year": years, metric: values})
                fig = px.line(
                    df_trend, 
                    x="Year", 
                    y=metric,
                    title=f"{metric} Trend for {industry} Industry"
                )
                st.plotly_chart(fig)
    
    with tab2:
        st.subheader("Predictive Insights")
        
        # Get business's products/services for forecasting
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            "SELECT name FROM products WHERE business_id = %s",
            (business_id,)
        )
        products = [p[0] for p in cur.fetchall()]
        
        if products:
            selected_product = st.selectbox("Select Product for Forecast", products)
            forecast_period = st.selectbox("Forecast Period", ["3 months", "6 months", "1 year"])
            
            if st.button("Generate Forecast"):
                with st.spinner("Generating market forecast..."):
                    time.sleep(3)  # Simulate analysis
                    
                    # Generate forecast using AI
                    forecast = ai_models.generate_text(
                        f"Create a {forecast_period} market forecast for {selected_product}. "
                        "Include growth projections, risks, and recommendations.",
                        max_length=1500
                    )
                    
                    st.subheader(f"Market Forecast for {selected_product}")
                    st.write(forecast)
                    
                    # Simulated forecast chart
                    if forecast_period == "3 months":
                        periods = ["Month 1", "Month 2", "Month 3"]
                    elif forecast_period == "6 months":
                        periods = ["Month 1", "Month 2", "Month 3", "Month 4", "Month 5", "Month 6"]
                    else:
                        periods = [f"Q{quarter}" for quarter in range(1, 5)]
                    
                    forecast_values = np.random.normal(loc=100, scale=20, size=len(periods)).cumsum()
                    
                    fig = px.line(
                        x=periods,
                        y=forecast_values,
                        title=f"{forecast_period} Sales Forecast",
                        labels={"x": "Period", "y": "Projected Sales"}
                    )
                    st.plotly_chart(fig)
                    
                    # Save forecast to database
                    cur.execute(
                        """INSERT INTO market_data 
                        (business_id, industry, metric, value, date, source) 
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                        (business_id, "Product", f"{selected_product}_forecast", float(forecast_values[-1]), datetime.now().date(), "GrowBis Forecast")
                    )
                    conn.commit()
        else:
            st.info("No products available for forecasting")
        
        cur.close()
        conn.close()
def chat_module(business_id, ai_models): # ai_models might not be used directly in chat
    st.header("💬 Messaging")

    conn = get_db_connection()
    cur = conn.cursor()

    # --- Mappings ---
    TYPE_MAP_DISPLAY_TO_DB = {
        "Businesses": "business",
        "Investors": "investor",
        "Service Providers": "service_provider"
    }
    TYPE_MAP_DB_TO_DISPLAY = {v: k for k, v in TYPE_MAP_DISPLAY_TO_DB.items()}


    # --- State variables ---
    if 'chat_partner_db_type' not in st.session_state:
        st.session_state.chat_partner_db_type = None
    if 'chat_partner_id' not in st.session_state:
        st.session_state.chat_partner_id = None
    if 'chat_partner_name' not in st.session_state:
        st.session_state.chat_partner_name = None

    # --- Sidebar for selecting chat partner ---
    st.sidebar.subheader("Start or Continue Chat")
    
    st.sidebar.write("**Recent Chats:**")
    try:
        query_recent_chats = """
        SELECT DISTINCT
            CASE
                WHEN sender_type = 'business' AND sender_id = %(current_business_id)s THEN receiver_type
                ELSE sender_type
            END as partner_type,
            CASE
                WHEN sender_type = 'business' AND sender_id = %(current_business_id)s THEN receiver_id
                ELSE sender_id
            END as partner_id
        FROM messages
        WHERE (sender_type = 'business' AND sender_id = %(current_business_id)s)
           OR (receiver_type = 'business' AND receiver_id = %(current_business_id)s);
        """
        cur.execute(query_recent_chats, {'current_business_id': business_id})
        recent_partners_db = cur.fetchall()
        
        recent_chat_options = {} # Display Name: (db_type, id)
        for p_type_db, p_id in recent_partners_db:
            partner_display_name = "Unknown"
            partner_table_name = ""
            if p_type_db == 'business':
                partner_table_name = 'businesses'
            elif p_type_db == 'investor':
                partner_table_name = 'investors'
            elif p_type_db == 'service_provider':
                partner_table_name = 'service_providers'
            
            if partner_table_name:
                # Ensure p_id is an integer for the query
                try:
                    p_id_int = int(p_id)
                    cur.execute(sql.SQL("SELECT name FROM {} WHERE id = %s").format(sql.Identifier(partner_table_name)), (p_id_int,))
                    name_result = cur.fetchone()
                    if name_result:
                        partner_display_name = f"{name_result[0]} ({TYPE_MAP_DB_TO_DISPLAY.get(p_type_db, p_type_db.capitalize())})"
                        recent_chat_options[partner_display_name] = (p_type_db, p_id_int)
                except ValueError:
                    st.sidebar.warning(f"Invalid ID format for partner type {p_type_db}: {p_id}")
                except Exception as e_name:
                    st.sidebar.warning(f"Could not fetch name for {p_type_db} ID {p_id}: {e_name}")


        selected_recent_chat_display = st.sidebar.selectbox(
            "Select from recent chats:",
            options=["-- New Chat --"] + list(recent_chat_options.keys()),
            key="select_recent_chat"
        )

        if selected_recent_chat_display != "-- New Chat --":
            partner_db_type, partner_id_val = recent_chat_options[selected_recent_chat_display]
            st.session_state.chat_partner_db_type = partner_db_type
            st.session_state.chat_partner_id = partner_id_val
            st.session_state.chat_partner_name = selected_recent_chat_display.split(" (")[0]
            st.rerun()

    except Exception as e_recent:
        st.sidebar.error(f"Error loading recent chats: {e_recent}")


    st.sidebar.markdown("---")
    st.sidebar.subheader("Find New Chat Partner")
    chat_type_options_display = ["Businesses", "Investors", "Service Providers"]
    selected_chat_type_display = st.sidebar.selectbox("Chat with:", chat_type_options_display, key="select_chat_type_display")

    partner_options_new = {} # Display Name: (db_type, id)
    
    try:
        target_db_type = TYPE_MAP_DISPLAY_TO_DB.get(selected_chat_type_display)
        if target_db_type == "business":
            cur.execute("SELECT id, name FROM businesses WHERE id != %s ORDER BY name", (business_id,))
            partners_list = cur.fetchall()
            for pid, pname in partners_list:
                partner_options_new[f"{pname} (Business)"] = (target_db_type, pid)
        elif target_db_type == "investor":
            cur.execute("SELECT id, name FROM investors ORDER BY name") # Show all investors
            partners_list = cur.fetchall()
            for pid, pname in partners_list:
                partner_options_new[f"{pname} (Investor)"] = (target_db_type, pid)
        elif target_db_type == "service_provider":
            cur.execute("SELECT id, name, service_type FROM service_providers ORDER BY name") # Show all
            partners_list = cur.fetchall()
            for pid, pname, p_service_type in partners_list:
                 partner_options_new[f"{pname} ({p_service_type.capitalize()} - Service Provider)"] = (target_db_type, pid)
    except Exception as e:
        st.sidebar.error(f"Error fetching new chat partners: {e}")

    selected_partner_display_new = st.sidebar.selectbox(
        f"Select {selected_chat_type_display[:-1]}:",
        options=["-- Select --"] + list(partner_options_new.keys()),
        key="select_new_chat_partner"
    )

    if selected_partner_display_new != "-- Select --":
        partner_db_type, partner_id_val = partner_options_new[selected_partner_display_new]
        st.session_state.chat_partner_db_type = partner_db_type
        st.session_state.chat_partner_id = partner_id_val
        st.session_state.chat_partner_name = selected_partner_display_new.split(" (")[0]
        st.rerun()

    # --- Main area for chat interface ---
    if st.session_state.chat_partner_db_type and st.session_state.chat_partner_id:
        partner_db_type = st.session_state.chat_partner_db_type
        partner_id = st.session_state.chat_partner_id
        partner_name = st.session_state.chat_partner_name

        st.subheader(f"Chat with {partner_name} ({TYPE_MAP_DB_TO_DISPLAY.get(partner_db_type, partner_db_type).rstrip('s')})")

        # --- Display Profile Snippet (Example) ---
        with st.expander(f"View {partner_name}'s Profile Snippet", expanded=False):
            try:
                profile_info = "Not available."
                if partner_db_type == 'business':
                    cur.execute("SELECT email, created_at FROM businesses WHERE id = %s", (partner_id,))
                    p_data = cur.fetchone()
                    if p_data: profile_info = f"Email: {p_data[0]}\nJoined: {p_data[1].strftime('%Y-%m-%d')}"
                elif partner_db_type == 'investor':
                    cur.execute("SELECT firm, email, investment_focus FROM investors WHERE id = %s", (partner_id,))
                    p_data = cur.fetchone()
                    if p_data: profile_info = f"Firm: {p_data[0]}\nEmail: {p_data[1]}\nFocus: {p_data[2]}"
                elif partner_db_type == 'service_provider':
                    cur.execute("SELECT service_type, contact_email, rating, experience_years FROM service_providers WHERE id = %s", (partner_id,))
                    p_data = cur.fetchone()
                    if p_data: profile_info = f"Service: {p_data[0].capitalize()}\nEmail: {p_data[1]}\nRating: {p_data[2]}/5\nExperience: {p_data[3]} years"
                st.text(profile_info)
            except Exception as e_prof:
                st.warning(f"Could not load profile snippet: {e_prof}")
        
        # --- Fetch and Display Messages ---
        current_user_db_type = "business" # Logged-in user is always a business
        current_user_id = business_id

        try:
            query_messages = """
                SELECT sender_type, sender_id, content, created_at
                FROM messages
                WHERE
                    (sender_type = %(current_type)s AND sender_id = %(current_id)s AND receiver_type = %(partner_type)s AND receiver_id = %(partner_id)s)
                OR
                    (sender_type = %(partner_type)s AND sender_id = %(partner_id)s AND receiver_type = %(current_type)s AND receiver_id = %(current_id)s)
                ORDER BY created_at ASC;
            """
            cur.execute(query_messages, {
                'current_type': current_user_db_type, 'current_id': current_user_id,
                'partner_type': partner_db_type, 'partner_id': partner_id
            })
            messages = cur.fetchall()

            chat_container = st.container()
            with chat_container:
                if not messages:
                    st.info(f"No messages yet with {partner_name}. Start the conversation!")

                for msg_sender_type, msg_sender_id, msg_content, msg_created_at in messages:
                    timestamp_str = msg_created_at.strftime('%Y-%m-%d %H:%M')
                    if msg_sender_type == current_user_db_type and msg_sender_id == current_user_id:
                        st.markdown(f"<div style='text-align: right; margin-left: 20%; margin-bottom: 5px; padding: 10px; background-color: #DCF8C6; border-radius: 10px;'><b>You</b> ({timestamp_str}):<br>{msg_content}</div>", unsafe_allow_html=True)
                    else:
                        st.markdown(f"<div style='text-align: left; margin-right: 20%; margin-bottom: 5px; padding: 10px; background-color: #FFFFFF; border-radius: 10px; border: 1px solid #E0E0E0;'><b>{partner_name}</b> ({timestamp_str}):<br>{msg_content}</div>", unsafe_allow_html=True)
            
            # Auto-scroll (basic attempt, might need more robust JS for perfect scroll)
            if messages:
                 st.markdown(f"<script>window.scrollTo(0,document.body.scrollHeight);</script>", unsafe_allow_html=True)


        except Exception as e:
            st.error(f"Error fetching messages: {e}")

        # --- Message Input Form ---
        with st.form("new_message_form", clear_on_submit=True):
            new_message_content = st.text_area("Your message:", key=f"new_message_content_{partner_id}") # Unique key
            send_button = st.form_submit_button("Send")

            if send_button and new_message_content.strip():
                try:
                    insert_query = """
                        INSERT INTO messages (sender_type, sender_id, receiver_type, receiver_id, content)
                        VALUES (%s, %s, %s, %s, %s);
                    """
                    cur.execute(insert_query, (
                        current_user_db_type, current_user_id,
                        partner_db_type, partner_id,
                        new_message_content.strip()
                    ))
                    conn.commit()
                    # st.success("Message sent!") # Can be removed for cleaner UI, rerun shows message
                    st.rerun()
                except Exception as e:
                    st.error(f"Error sending message: {e}")
            elif send_button and not new_message_content.strip():
                st.warning("Message cannot be empty.")
    else:
        st.info("Select a user, investor, or service provider from the sidebar to start chatting, or choose a recent chat.")

    cur.close()
    conn.close()

# Main Application
def main():
    
    st.set_page_config(
        page_title="GrowBis", 
        page_icon="🚀", 
        layout="wide",
        initial_sidebar_state="expanded"
    )
    # Initialize database and AI models
    init_db()
    
    ai_models=load_ai_models()
    
    # Custom CSS
    st.markdown("""
    <style>
    .main {
        background-color: #f5f5f5;
    }
    .sidebar .sidebar-content {
        background-color: #2c3e50;
        color: white;
    }
    h1 {
        color: #2c3e50;
    }
    .stButton>button {
        background-color: #3498db;
        color: white;
    }
    .stDownloadButton>button {
        background-color: #2ecc71;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True)
    
    if 'show_registration' in st.session_state:
        reg_type = st.session_state.show_registration
        # Clear it so it doesn't persist on subsequent reruns unless explicitly set
        del st.session_state.show_registration 
        
        if reg_type == 'investor':
            investor_registration_page()
            if st.button("Back to Login"):
                 st.rerun() # Go back to login selection
            return 
        elif reg_type == 'service_provider':
            service_provider_registration_page()
            if st.button("Back to Login"):
                 st.rerun()
            return
        # Business registration is handled within login_page's tabs

    if not check_auth():
        login_page()
        return

    # --- User is Authenticated ---
    user_type = st.session_state.user_type
    user_name = st.session_state.user_name
    entity_id = st.session_state.logged_in_entity_id

    st.sidebar.title(f"Welcome, {user_name}!")
    if st.sidebar.button("Logout"):
        logout() # Your existing logout function
        st.rerun()
    
    if user_type == 'business':
        # Existing business dashboard logic
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT name, subscription_type, subscription_expiry FROM businesses WHERE id = %s", (entity_id,))
        business_info = cur.fetchone()
        cur.close()
        conn.close()
        
        business_name_display = business_info[0]
        subscription_type_display = business_info[1]
        subscription_expiry_display = business_info[2]
        
        days_left = (subscription_expiry_display - datetime.now().date()).days
        subscription_status = "Expired" if days_left < 0 else (f"Expires in {days_left} days" if days_left < 30 else f"Active until {subscription_expiry_display}")
        
        st.title(f"🚀 {business_name_display} - GrowBis Business Platform")
        st.markdown(f"**Subscription:** {subscription_type_display.capitalize()} • {subscription_status}")
        
        st.sidebar.markdown("---")
        st.sidebar.subheader("Business Modules")
        modules = [
            "Dashboard", "Inventory & Billing", "HR Tools", "Project Manager", 
            "Document Generator", "Market Analysis Tool", "Market Doubt Assistant (AI Chatbot)",
            "Investor & Agent Dashboards", "Govt/Private Schemes & News Alerts", 
            "Opportunity Director", "Voice Navigation", "Pitching Helper", 
            "Strategy Generator", "Hiring Helper", "Tax & GST Filing", 
            "IPO & Cap Table Management", "Legal, CA & Insurance Marketplace",
            "Enterprise Intelligence Dashboards", "AI Market Forecasting", 
            "Messaging" # Added comma here
        ]
        selected_module = st.sidebar.selectbox("Select Module", modules, key="business_module_select")

        # Business module routing (your existing if/elif chain)
        if selected_module == "Dashboard":
            st.header("📊 Dashboard")
            st.write("### Business Overview (Quarterly)")
            today = datetime.now().date()
            cq_start, cq_end = get_quarter_dates(today)
            pq_start, pq_end = get_previous_quarter_dates(today)
            current_q_revenue = get_dashboard_financials(entity_id, cq_start, cq_end)
            prev_q_revenue = get_dashboard_financials(entity_id, pq_start, pq_end)
            total_monthly_salary = get_total_monthly_salary_expense(entity_id)
            current_q_expenses_est = total_monthly_salary * 3
            prev_q_expenses_est = total_monthly_salary * 3
            current_q_profit_est = current_q_revenue - current_q_expenses_est
            prev_q_profit_est = prev_q_revenue - prev_q_expenses_est

            def format_currency(value):
                return f"${value:,.0f}"

            def calculate_delta_string(current, previous):
                if previous == 0 and current > 0: return "New"
                if previous == 0 and current == 0: return "0%"
                if previous == 0 and current < 0: return "-100%" 
                if previous != 0:
                    percentage_change = ((current - previous) / abs(previous)) * 100
                    return f"{percentage_change:.1f}%"
                return "N/A"

            col1, col2, col3 = st.columns(3)
            with col1: st.metric("Revenue (Current Qtr)", format_currency(current_q_revenue), f"{calculate_delta_string(current_q_revenue, prev_q_revenue)} vs Prev. Qtr")
            with col2: 
                st.metric("Est. Expenses (Current Qtr)", format_currency(current_q_expenses_est), f"{calculate_delta_string(current_q_expenses_est, prev_q_expenses_est)} vs Prev. Qtr (Est.)")
                st.caption("Expenses estimated based on 3x current monthly salaries.")
            with col3: st.metric("Est. Profit (Current Qtr)", format_currency(current_q_profit_est), f"{calculate_delta_string(current_q_profit_est, prev_q_profit_est)} vs Prev. Qtr (Est.)")
            
            st.write("### Recent Activity")
            activities = get_recent_activities_for_dashboard(entity_id, limit=5)
            if activities:
                for activity in activities:
                    with st.expander(f"{activity['type']}: {activity['detail']}"): st.write(f"⏱️ {activity['time_string']}")
            else: st.info("No recent activity.")

        elif selected_module == "Inventory & Billing": inventory_module(entity_id, ai_models)
        elif selected_module == "HR Tools": hr_module(entity_id, ai_models)
        elif selected_module == "Project Manager": project_module(entity_id, ai_models)
        elif selected_module == "Document Generator": document_module(entity_id, ai_models)
        elif selected_module == "Market Analysis Tool": market_analysis_module(entity_id, ai_models)
        elif selected_module == "Market Doubt Assistant (AI Chatbot)": chatbot_module(entity_id, ai_models)
        elif selected_module == "Investor & Agent Dashboards": investor_dashboard(entity_id, ai_models)
        elif selected_module == "Govt/Private Schemes & News Alerts": schemes_module(entity_id, ai_models)
        elif selected_module == "Opportunity Director": opportunities_module(entity_id, ai_models)
        elif selected_module == "Voice Navigation": voice_navigation(entity_id, ai_models)
        elif selected_module == "Pitching Helper": pitching_helper(entity_id, ai_models)
        elif selected_module == "Strategy Generator": strategy_generator(entity_id, ai_models)
        elif selected_module == "Hiring Helper": hiring_helper(entity_id, ai_models)
        elif selected_module == "Tax & GST Filing": tax_module(entity_id, ai_models)
        elif selected_module == "IPO & Cap Table Management": ipo_module(entity_id, ai_models)
        elif selected_module == "Legal, CA & Insurance Marketplace": 
            legal_marketplace(entity_id, ai_models)
            st.info("Adding a legal provider here makes their profile (Name, Service, Contact) potentially visible to other businesses on GrowBis for chat-based inquiries.")
        elif selected_module == "Enterprise Intelligence Dashboards": enterprise_intelligence(entity_id, ai_models)
        elif selected_module == "AI Market Forecasting": market_forecasting(entity_id, ai_models)
        elif selected_module == "Messaging":
            chat_module(entity_id, ai_models) 
        else:
            st.write(f"Module {selected_module} selected.")


    elif user_type == 'investor':
        st.sidebar.markdown("---")
        st.sidebar.subheader("Investor Tools")
        investor_portal(entity_id, ai_models)
        
    elif user_type == 'service_provider':
        st.sidebar.markdown("---")
        st.sidebar.subheader(f"{st.session_state.service_type.capitalize()} Tools")
        service_provider_portal(entity_id, ai_models)

    # Footer (common to all logged-in users)
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    ### About GrowBis
    - **Version**: 2.1 (Multi-Role Update)
    """)

if __name__ == "__main__":
    main()
