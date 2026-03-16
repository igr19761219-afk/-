import sqlite3
import json
from datetime import datetime, timedelta
import random
import logging

DB_NAME = "finbot.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY,
                  username TEXT,
                  first_name TEXT,
                  invited_by INTEGER,
                  reg_date TEXT,
                  last_active TEXT,
                  test_completed INTEGER DEFAULT 0,
                  test_score INTEGER,
                  test_answers TEXT,
                  feedback TEXT,
                  video_sent INTEGER DEFAULT 0,
                  video_confirmed INTEGER DEFAULT 0,
                  reminder_job_id TEXT,
                  meeting_format TEXT,
                  meeting_time TEXT,
                  meeting_city TEXT,
                  meeting_timestamp INTEGER,
                  meeting_reminder_sent INTEGER DEFAULT 0,
                  friend_responded INTEGER DEFAULT 0,
                  drip_stage INTEGER DEFAULT 0)''')
    conn.commit()

    c.execute("PRAGMA table_info(users)")
    existing_columns = [col[1] for col in c.fetchall()]

    new_columns = [
        ('user_type', 'TEXT DEFAULT \'new\''),
        ('mentor_approved', 'INTEGER DEFAULT 0'),
        ('mentor_id', 'INTEGER'),
        ('partner_code', 'TEXT'),
        ('chosen_path', 'TEXT'),
        ('blocked', 'INTEGER DEFAULT 0'),
        ('curator_status', 'TEXT DEFAULT \'active\''),
        ('curator_poll_sent', 'INTEGER DEFAULT 0'),
        ('last_drip_time', 'INTEGER'),
    ]

    for col_name, col_def in new_columns:
        if col_name not in existing_columns:
            try:
                c.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_def}")
                logging.info(f"✅ Добавлена колонка {col_name} в таблицу users")
            except Exception as e:
                logging.error(f"❌ Ошибка добавления колонки {col_name}: {e}")

    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_partner_code ON users(partner_code) WHERE partner_code IS NOT NULL")
    conn.commit()
    conn.close()

# ---------- Базовые функции ----------
def add_user(user_id, username, first_name, invited_by=None):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute('''INSERT OR IGNORE INTO users 
                 (user_id, username, first_name, invited_by, reg_date, last_active)
                 VALUES (?,?,?,?,?,?)''',
              (user_id, username, first_name, invited_by, now, now))
    conn.commit()
    conn.close()

def update_last_active(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET last_active=? WHERE user_id=?", (datetime.now().isoformat(), user_id))
    conn.commit()
    conn.close()

def get_user(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row

def get_invited_by(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT invited_by FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def get_inviter_name(inviter_id):
    if not inviter_id:
        return None
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT first_name FROM users WHERE user_id=?", (inviter_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def get_username(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT username FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def set_test_result(user_id, score, cat_scores, feedback):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''UPDATE users SET test_completed=1, test_score=?, test_answers=?, feedback=?
                 WHERE user_id=?''',
              (score, json.dumps(cat_scores), feedback, user_id))
    conn.commit()
    conn.close()

def set_video_sent(user_id, job_id=None):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET video_sent=1, reminder_job_id=? WHERE user_id=?", (job_id, user_id))
    conn.commit()
    conn.close()

def confirm_video(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET video_confirmed=1, reminder_job_id=NULL WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def set_meeting(user_id, fmt, time_str, city, timestamp):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''UPDATE users SET 
                 meeting_format=?, meeting_time=?, meeting_city=?, meeting_timestamp=?,
                 meeting_reminder_sent=0, curator_poll_sent=0, friend_responded=0, drip_stage=0, last_drip_time=NULL
                 WHERE user_id=?''',
              (fmt, time_str, city, timestamp, user_id))
    conn.commit()
    conn.close()

def mark_meeting_reminder_sent(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET meeting_reminder_sent=1 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def set_friend_responded(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET friend_responded=1 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def set_drip_stage(user_id, stage):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET drip_stage=? WHERE user_id=?", (stage, user_id))
    conn.commit()
    conn.close()

# ---------- Реферальные функции ----------
def set_user_ready(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET user_type='candidate' WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def approve_partner(user_id, mentor_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    partner_code = generate_partner_code()
    c.execute('''UPDATE users SET user_type='partner', mentor_approved=1, mentor_id=?, partner_code=?
                 WHERE user_id=?''', (mentor_id, partner_code, user_id))
    conn.commit()
    conn.close()
    return partner_code

def reject_candidate(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET user_type='rejected' WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def get_candidates(inviter_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''SELECT user_id, first_name, username, reg_date FROM users
                 WHERE invited_by=? AND user_type='candidate' AND mentor_approved=0''', (inviter_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def get_partners(inviter_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''SELECT user_id, first_name, username, reg_date FROM users
                 WHERE invited_by=? AND user_type='partner' AND mentor_approved=1''', (inviter_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def get_inviter_stats(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users WHERE invited_by=?", (user_id,))
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM users WHERE invited_by=? AND user_type='partner' AND mentor_approved=1", (user_id,))
    active = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM users WHERE invited_by=? AND user_type='candidate' AND mentor_approved=0", (user_id,))
    pending = c.fetchone()[0]
    conn.close()
    return total, active, pending

def get_partner_code(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT partner_code FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def generate_partner_code():
    letters = random.choice(['SV', 'ALFA', 'TEAM', 'PRO'])
    numbers = random.randint(100, 999)
    return f"{letters}-{numbers}"

def ensure_partner_code(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT partner_code FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    if row and row[0]:
        code = row[0]
    else:
        code = generate_partner_code()
        c.execute("UPDATE users SET partner_code=? WHERE user_id=?", (code, user_id))
        conn.commit()
    conn.close()
    return code

def set_chosen_path(user_id, path):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET chosen_path=? WHERE user_id=?", (path, user_id))
    conn.commit()
    conn.close()

def set_user_blocked(user_id, blocked=1):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET blocked=? WHERE user_id=?", (blocked, user_id))
    conn.commit()
    conn.close()

# ---------- Функции для статуса curator_status ----------
def set_curator_status(user_id, status):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE users SET curator_status=? WHERE user_id=?", (status, user_id))
    conn.commit()
    conn.close()

def get_curator_status(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT curator_status FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 'active'

def cancel_user_jobs(user_id, context):
    """Отменяет все запланированные задачи для пользователя (опрос, автодогрев, догрев)"""
    if not context.job_queue:
        return
    for job in context.job_queue.jobs():
        if job.name in [f"ask_friend_{user_id}", f"auto_drip_{user_id}"]:
            job.schedule_removal()
        if job.name and job.name.startswith(f"drip_{user_id}_"):
            job.schedule_removal()

# ---------- Функции для молчунов и блокированных ----------
def get_silent_candidates(inviter_id, days=3):
    """Возвращает список активных кандидатов, неактивных более days дней."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    c.execute('''
        SELECT user_id, first_name, username, last_active, chosen_path,
               test_completed, feedback, video_sent, meeting_time
        FROM users
        WHERE invited_by=? AND user_type != 'partner' AND last_active < ? 
          AND curator_status='active'
        ORDER BY last_active ASC
    ''', (inviter_id, cutoff))
    rows = c.fetchall()
    conn.close()
    return rows

def get_blocked_candidates(inviter_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        SELECT user_id, first_name, username, last_active, chosen_path
        FROM users
        WHERE invited_by=? AND curator_status='blocked'
        ORDER BY last_active DESC
    ''', (inviter_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def get_inactive_candidates(inviter_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        SELECT user_id, first_name, username, last_active, chosen_path
        FROM users
        WHERE invited_by=? AND curator_status='inactive'
        ORDER BY last_active DESC
    ''', (inviter_id,))
    rows = c.fetchall()
    conn.close()
    return rows

def get_user_dict(user_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        SELECT user_id, username, first_name, invited_by, last_active,
               test_completed, test_score, test_answers, feedback,
               video_sent, video_confirmed, meeting_format, meeting_time,
               meeting_city, meeting_timestamp, user_type, mentor_approved,
               mentor_id, partner_code, chosen_path, blocked, curator_status
        FROM users WHERE user_id=?
    ''', (user_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    columns = ['user_id', 'username', 'first_name', 'invited_by', 'last_active',
               'test_completed', 'test_score', 'test_answers', 'feedback',
               'video_sent', 'video_confirmed', 'meeting_format', 'meeting_time',
               'meeting_city', 'meeting_timestamp', 'user_type', 'mentor_approved',
               'mentor_id', 'partner_code', 'chosen_path', 'blocked', 'curator_status']
    return dict(zip(columns, row))