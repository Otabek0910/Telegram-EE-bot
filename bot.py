# =============================================================================
# ШАГ 1: ИМПОРТЫ
# =============================================================================
import logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
from datetime import time

from localization import get_text, get_data_translation

import pytz
import os 
import math
import psycopg2
from psycopg2 import sql
from datetime import datetime
from datetime import date, timedelta
from openpyxl.utils import get_column_letter
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv
from telegram.helpers import escape_markdown
from sqlalchemy import create_engine, text
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.helpers import escape_markdown
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler,
    ConversationHandler,
)

# --- НАСТРОЙКИ ---
load_dotenv()

TOKEN = os.getenv("TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
# DATABASE_URL = "postgresql://postgres:9137911@localhost:5432/Bot_Telegram_Brigads" - Для Локалки - раскоментировать
DATABASE_URL = os.getenv("DATABASE_URL") # Для хостинга - раскоментировать
REPORTS_PER_PAGE = 5
NORM_PER_PERSON = 5 # Условная норма выработка - уже не требуется!
USERS_PER_PAGE = 10
ELEMENTS_PER_PAGE = 10
GETTING_HR_DATE = 30
(SELECT_DISC_FOR_EDIT, SELECT_BRIGADE_FOR_EDIT, SELECT_REPORT_FOR_EDIT,
 CONFIRM_DELETE, EDIT_REPORT_START, EDIT_REPORT_FIELD) = range(30, 36)

SELECT_FIELD_TO_EDIT, AWAITING_NEW_VALUE, CONFIRM_EDIT = range(40, 43)

ALL_TABLE_NAMES_FOR_BACKUP = [
    'disciplines', 'construction_objects', 'work_types', 'admins', 'managers', 
    'brigades', 'pto', 'kiok', 'reports', 'topic_mappings', 'personnel_roles', 
    'daily_rosters', 'daily_roster_details'
]

TEMP_DIR = 'temp_files'
DASHBOARD_DIR = 'dashboards'
BACKUP_DIR = 'database_backups'      
BACKUP_RETENTION_DAYS = 7          
REPORTS_GROUP_URL = "https://t.me/+OdHnUNt1WaZiMDY6" 

AWAITING_RESTORE_FILE = range(12, 13)
AWAITING_DISCIPLINE_FOR_MANAGER = range(23, 24)
SELECTING_PERSONNEL_HISTORY_DATE = range(24, 25)
(SELECTING_OVERVIEW_ACTION, AWAITING_OVERVIEW_DATE) = range(50, 52)


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"), 
        logging.StreamHandler()         
    ]
)
logger = logging.getLogger(__name__)


SELECTING_ROLE, GETTING_NAME, GETTING_CONTACT, SELECTING_MANAGER_LEVEL, SELECTING_DISCIPLINE = range(5)

AWAITING_ROLES_COUNT, CONFIRM_ROSTER, CONFIRM_DANGEROUS_ROSTER_SAVE = range(20, 23) 

OWNER_SELECTING_DISCIPLINE, GETTING_CORPUS, GETTING_WORK_TYPE, GETTING_PEOPLE_COUNT, GETTING_VOLUME, GETTING_DATE, GETTING_NOTES, CONFIRM_REPORT = range(5, 13)


# --- РАБОТА С БАЗОЙ ДАННЫХ ---

def init_db():
    """Инициализация базы данных PostgreSQL."""
    if not DATABASE_URL:
        logger.error("Переменная DATABASE_URL не определена в коде! Инициализация невозможна.")
        return
    
    conn_str = DATABASE_URL

    create_commands = [
        'DROP TABLE IF EXISTS admins, brigades, pto, reports, managers, kiok, construction_objects, work_types, disciplines, topic_mappings, personnel_roles, daily_rosters, daily_roster_details CASCADE',
        '''CREATE TABLE disciplines (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)''',
        '''CREATE TABLE construction_objects (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, display_order INTEGER DEFAULT 0)''',
        '''CREATE TABLE work_types (id SERIAL PRIMARY KEY, name TEXT NOT NULL, discipline_id INTEGER NOT NULL REFERENCES disciplines(id), unit_of_measure TEXT, norm_per_unit REAL, display_order INTEGER DEFAULT 0)''',
        '''CREATE TABLE admins (user_id VARCHAR(255) PRIMARY KEY, first_name TEXT, last_name TEXT, username TEXT, phone_number TEXT, language_code VARCHAR(2) DEFAULT 'ru')''',
        '''CREATE TABLE managers (user_id VARCHAR(255) PRIMARY KEY, level INTEGER NOT NULL, discipline INTEGER REFERENCES disciplines(id), first_name TEXT, last_name TEXT, username TEXT, phone_number TEXT, language_code VARCHAR(2) DEFAULT 'ru')''',
        '''CREATE TABLE brigades (user_id VARCHAR(255) PRIMARY KEY, brigade_name TEXT, discipline INTEGER REFERENCES disciplines(id), first_name TEXT, last_name TEXT, username TEXT, phone_number TEXT, language_code VARCHAR(2) DEFAULT 'ru')''',
        '''CREATE TABLE pto (user_id VARCHAR(255) PRIMARY KEY, discipline INTEGER REFERENCES disciplines(id), first_name TEXT, last_name TEXT, username TEXT, phone_number TEXT, language_code VARCHAR(2) DEFAULT 'ru')''',
        '''CREATE TABLE kiok (user_id VARCHAR(255) PRIMARY KEY, discipline INTEGER REFERENCES disciplines(id), first_name TEXT, last_name TEXT, username TEXT, phone_number TEXT, language_code VARCHAR(2) DEFAULT 'ru')''',
        '''CREATE TABLE reports (id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ DEFAULT NOW(), corpus_name TEXT, discipline_name TEXT, work_type_name TEXT, foreman_name TEXT, people_count INTEGER, volume REAL, report_date DATE, notes TEXT, kiok_approved INTEGER DEFAULT 0, kiok_approver_id VARCHAR(255), kiok_approval_timestamp TIMESTAMPTZ, group_message_id BIGINT)''',
        '''CREATE TABLE topic_mappings (discipline_name TEXT PRIMARY KEY, chat_id BIGINT NOT NULL, topic_id INTEGER NOT NULL)''',
        '''CREATE TABLE personnel_roles (
            id SERIAL PRIMARY KEY,
            role_name TEXT NOT NULL,
            discipline_id INTEGER NOT NULL REFERENCES disciplines(id),
            UNIQUE (role_name, discipline_id) 
        )''',
        
        
        '''CREATE TABLE daily_rosters (
            id SERIAL PRIMARY KEY,
            roster_date DATE NOT NULL,
            brigade_user_id VARCHAR(255) NOT NULL REFERENCES brigades(user_id) ON DELETE CASCADE,
            total_people INTEGER NOT NULL,
            UNIQUE (roster_date, brigade_user_id)
        )''',
        
        
        '''CREATE TABLE daily_roster_details (
            id SERIAL PRIMARY KEY,
            roster_id INTEGER NOT NULL REFERENCES daily_rosters(id) ON DELETE CASCADE,
            role_id INTEGER NOT NULL REFERENCES personnel_roles(id),
            people_count INTEGER NOT NULL
        )''',
    ]
    
    conn = None
    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
              
        for command in create_commands:
            cursor.execute(command)
        logger.info("Таблицы в PostgreSQL успешно созданы.")

        # Шаблончик
        initial_disciplines = [('МК',), ('Общестрой',), ('Труба',), ('Архитектура',)]
        cursor.executemany("INSERT INTO disciplines (name) VALUES (%s)", initial_disciplines)
               
        cursor.execute("SELECT name, id FROM disciplines")
        disciplines_map = {name: i for name, i in cursor.fetchall()}

        initial_objects = [('Корпус 1',), ('Корпус 2',), ('Корпус 5А',), ('КПП',)]
        cursor.executemany("INSERT INTO construction_objects (name) VALUES (%s)", initial_objects)

        initial_work_types = [
            ('Монтаж колонн', disciplines_map['МК'], 'тонн', 5.0),
            ('Монтаж ферм', disciplines_map['МК'], 'м.п.', 10.0),
            ('Бетонные работы', disciplines_map['Общестрой'], 'м³', 1.5),
            ('Кладочные работы', disciplines_map['Общестрой'], 'м²', 12.0),
            ('Монтаж трубопровода', disciplines_map['Труба'], 'м.п.', 8.0),
            ('Сварка стыков', disciplines_map['Труба'], 'шт.', 20.0),
            ('Монтаж фасада', disciplines_map['Архитектура'], 'м²', 7.0),
        ]
        cursor.executemany("INSERT INTO work_types (name, discipline_id, unit_of_measure, norm_per_unit) VALUES (%s, %s, %s, %s)", initial_work_types)
        
        logger.info("Таблицы-справочники успешно наполнены данными.")

         # Шаблончик
        initial_roles = [
           
            ('Сварщик', disciplines_map['Труба']),
            ('Монтажник', disciplines_map['Труба']),
            
            ('Работнки', disciplines_map['МК']),
            ('Работник', disciplines_map['Общестрой']),
            ('Работник', disciplines_map['Архитектура'])
        ]
        cursor.executemany("INSERT INTO personnel_roles (role_name, discipline_id) VALUES (%s, %s) ON CONFLICT (role_name, discipline_id) DO NOTHING", initial_roles)
        logger.info("Справочник должностей успешно наполнен.")

        conn.commit()
        cursor.close()
    except psycopg2.Error as e:
        logger.error(f"Ошибка при инициализации PostgreSQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
    
def db_query(query: str, params: tuple = ()):
    """
    ИСПРАВЛЕННАЯ ВЕРСИЯ:
    Универсальная функция, которая теперь корректно работает и с текстовыми
    запросами, и с форматированными объектами psycopg2.sql.
    """
    if not DATABASE_URL:
        logger.error("Переменная DATABASE_URL не определена в коде!")
        return None
    
    result = None
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute(query, params)
       
        is_select_query = False
        is_returning_query = False
        if isinstance(query, str):
            query_upper = query.strip().upper()
            if query_upper.startswith("SELECT"):
                is_select_query = True
            elif "RETURNING" in query_upper:
                is_returning_query = True
        
        if is_select_query:
            result = cursor.fetchall()
        elif is_returning_query:
            result = cursor.fetchone()[0]
           
        conn.commit()
        cursor.close()
    except Exception as e:
        
        logger.error(f"Ошибка базы данных PostgreSQL: {e}\nЗапрос: {query}\nПараметры: {params}")
        if conn: conn.rollback()
        return None
    finally:
        if conn: conn.close()
    return result

def ensure_dirs_exist():
    """Проверяет и создает необходимые директории для файлов."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(DASHBOARD_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    logger.info(f"Проверены и созданы директории: {TEMP_DIR}/ и {DASHBOARD_DIR}/")

def check_user_role(user_id: str) -> dict:
    """Проверяет все таблицы ролей и возвращает подробный объект с правами (PostgreSQL-совместимая версия)."""
    role_info = {
        'isAdmin': False, 'isManager': False, 'managerLevel': None,
        'isForeman': False, 'isPto': False, 'isKiok': False,
        'discipline': None, 'brigadeName': None, 'phoneNumber': None
    }
    
    if user_id == OWNER_ID:
        role_info.update({'isAdmin': True, 'isManager': True, 'managerLevel': 1})
        return role_info
  
    admin_check = db_query("SELECT phone_number FROM admins WHERE user_id = %s", (user_id,))
    if admin_check:
        role_info['isAdmin'] = True
        if not role_info['phoneNumber']: role_info['phoneNumber'] = admin_check[0][0]

    manager_check = db_query("SELECT m.level, d.name, m.phone_number FROM managers m LEFT JOIN disciplines d ON m.discipline = d.id WHERE m.user_id = %s", (user_id,))
    if manager_check:
        role_info['isManager'] = True
        level, discipline_name, phone = manager_check[0]
        role_info['managerLevel'] = level
        if not role_info['discipline']: role_info['discipline'] = discipline_name
        if not role_info['phoneNumber']: role_info['phoneNumber'] = phone
    
    brigade_check = db_query("SELECT b.brigade_name, d.name, b.phone_number FROM brigades b LEFT JOIN disciplines d ON b.discipline = d.id WHERE b.user_id = %s", (user_id,))
    if brigade_check:
        role_info['isForeman'] = True
        brigade_name, discipline_name, phone = brigade_check[0]
        role_info['brigadeName'] = brigade_name
        if not role_info['discipline']: role_info['discipline'] = discipline_name
        if not role_info['phoneNumber']: role_info['phoneNumber'] = phone
        
    pto_check = db_query("SELECT d.name, p.phone_number FROM pto p LEFT JOIN disciplines d ON p.discipline = d.id WHERE p.user_id = %s", (user_id,))
    if pto_check:
        role_info['isPto'] = True
        discipline_name, phone = pto_check[0]
        if not role_info['discipline']: role_info['discipline'] = discipline_name
        if not role_info['phoneNumber']: role_info['phoneNumber'] = phone
        
    kiok_check = db_query("SELECT d.name, k.phone_number FROM kiok k LEFT JOIN disciplines d ON k.discipline = d.id WHERE k.user_id = %s", (user_id,))
    if kiok_check:
        role_info['isKiok'] = True
        discipline_name, phone = kiok_check[0]
        if not role_info['discipline']: role_info['discipline'] = discipline_name
        if not role_info['phoneNumber']: role_info['phoneNumber'] = phone
        
    return role_info

def update_user_role(user_id: str, role: str, user_info: dict, discipline: int = None, level: int = None):
    """Сохраняет или обновляет информацию о пользователе в PostgreSQL."""
    first_name = user_info.get('first_name', '')
    last_name = user_info.get('last_name', '')
    username = user_info.get('username', 'не указан')
    phone_number = user_info.get('phone_number', '')
    
    logger.info(f"Сохранение роли для {user_id}: роль={role}, дисциплина={discipline}, уровень={level}")

    if role == 'admin':
        query = """
            INSERT INTO admins (user_id, first_name, last_name, username, phone_number)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                username = EXCLUDED.username,
                phone_number = EXCLUDED.phone_number;
        """
        params = (user_id, first_name, last_name, username, phone_number)
    
    elif role == 'manager':
        query = """
            INSERT INTO managers (user_id, level, discipline, first_name, last_name, username, phone_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                level = EXCLUDED.level,
                discipline = EXCLUDED.discipline,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                username = EXCLUDED.username,
                phone_number = EXCLUDED.phone_number;
        """
        params = (user_id, level, discipline, first_name, last_name, username, phone_number)

    elif role == 'foreman':
        brigade_name = f"{first_name} {last_name}"
        query = """
            INSERT INTO brigades (user_id, brigade_name, discipline, first_name, last_name, username, phone_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                brigade_name = EXCLUDED.brigade_name,
                discipline = EXCLUDED.discipline,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                username = EXCLUDED.username,
                phone_number = EXCLUDED.phone_number;
        """
        params = (user_id, brigade_name, discipline, first_name, last_name, username, phone_number)

    elif role == 'pto':
        query = """
            INSERT INTO pto (user_id, discipline, first_name, last_name, username, phone_number)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                discipline = EXCLUDED.discipline,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                username = EXCLUDED.username,
                phone_number = EXCLUDED.phone_number;
        """
        params = (user_id, discipline, first_name, last_name, username, phone_number)

    elif role == 'kiok':
        query = """
            INSERT INTO kiok (user_id, discipline, first_name, last_name, username, phone_number)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                discipline = EXCLUDED.discipline,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                username = EXCLUDED.username,
                phone_number = EXCLUDED.phone_number;
        """
        params = (user_id, discipline, first_name, last_name, username, phone_number)
    else:
        return

    db_query(query, params)

async def send_approval_request(context: ContextTypes.DEFAULT_TYPE, user_id_str: str, request_text: str, approve_callback: str, reject_callback: str):
    """Отправляет запрос на согласование всем администраторам и Овнеру."""
    keyboard = [
        [InlineKeyboardButton("✅ Подтвердить", callback_data=approve_callback)],
        [InlineKeyboardButton("❌ Отклонить", callback_data=reject_callback)]
    ]

    admin_ids_raw = db_query("SELECT user_id FROM admins")
    admin_ids = [row[0] for row in admin_ids_raw] if admin_ids_raw else []
  
    all_approvers = list(set(admin_ids + [OWNER_ID]))

    for admin_id in all_approvers:
        try:
            await context.bot.send_message(admin_id, request_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Не удалось отправить запрос на согласование админу {admin_id} для пользователя {user_id_str}: {e}")

# --- ОСНОВНЫЕ ФУНКЦИИ БОТА ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатывает команду /start. 
    В личном чате отправляет главное меню. 
    В группе пытается удалить команду.
    """
    chat_type = update.effective_chat.type
    
    if chat_type == 'private':
        await show_main_menu_logic(
            context, 
            user_id=str(update.effective_user.id), 
            chat_id=update.effective_chat.id
        )
   
    else:
        try:
            await update.message.delete()
            logger.info(f"Удалена команда /start в чате {update.effective_chat.id}")
        except Exception as e:
            logger.info(f"Не удалось удалить /start в группе (возможно, нет прав): {e}")

async def start_over(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Принудительно завершает любой активный диалог по команде /start
    и показывает главное меню.
    """
    user_id = str(update.effective_user.id)
    chat_id = update.effective_chat.id
    
    logger.info(f"Пользователь {user_id} использовал /start для сброса диалога.")
    
   
    context.user_data.clear()
    
   
    await show_main_menu_logic(context, user_id, chat_id)
    
   
    return ConversationHandler.END

async def remove_message_job(context: ContextTypes.DEFAULT_TYPE):
    """Задача для удаления сообщения по расписанию."""
    job_data = context.job.data
    chat_id = job_data['chat_id']
    message_id = job_data['message_id']
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"Сообщение {message_id} в чате {chat_id} удалено по расписанию.")
    except Exception as e:
        logger.warning(f"Не удалось удалить сообщение {message_id} по расписанию: {e}")

async def show_main_menu_logic(context: ContextTypes.DEFAULT_TYPE, user_id: str, chat_id: int, message_id_to_edit: int = None, greeting: str = None):
    """
    Основная логика для отображения главного меню (МНОГОЯЗЫЧНАЯ ВЕРСИЯ).
    """
    user_role = check_user_role(user_id)
    lang = get_user_language(user_id) 
    
    keyboard_buttons = []
    roster_summary_text = "" 

    if user_role['isForeman']:
        today_str = date.today().strftime('%Y-%m-%d')
        roster_info = db_query("SELECT id, total_people FROM daily_rosters WHERE brigade_user_id = %s AND roster_date = %s", (user_id, today_str))
        
        if roster_info:
            roster_id, total_declared = roster_info[0]
            
            details_raw = db_query("""
                SELECT pr.role_name, drd.people_count
                FROM daily_roster_details drd
                JOIN personnel_roles pr ON drd.role_id = pr.id
                WHERE drd.roster_id = %s
            """, (roster_id,))
            details_text = ", ".join([f"{name}: {count}" for name, count in details_raw]) if details_raw else "детали не найдены"
            
            brigade_name_for_query = user_role.get('brigadeName') or f"Бригада пользователя {user_id}"
            assigned_info = db_query("SELECT SUM(people_count) FROM reports WHERE foreman_name = %s AND report_date = %s", (brigade_name_for_query, today_str))
            total_assigned = assigned_info[0][0] or 0 if assigned_info else 0
            
            reserve = total_declared - total_assigned
            
            
            roster_summary_text = f"\n\n{get_text('main_menu_roster_summary_compact', lang).format(total=total_declared, reserve=reserve)}"

    # --- Логика отображения кнопок с использованием get_text ---
    if user_role['isForeman'] and not roster_summary_text:
        keyboard_buttons.append([InlineKeyboardButton(get_text('submit_roster_button', lang), callback_data="submit_roster")])

    if user_role['isForeman']:
        keyboard_buttons.append([InlineKeyboardButton(get_text('form_report_button', lang), callback_data="new_report")])

    if any([user_role['isManager'], user_role['isPto'], user_role['isKiok'], user_role['isForeman']]):
        keyboard_buttons.append([InlineKeyboardButton(get_text('view_reports_button', lang), callback_data="report_menu_all")])
    
    if any([user_role['isAdmin'], user_role['isManager'], user_role['isForeman'], user_role['isPto'], user_role['isKiok']]):
        keyboard_buttons.append([InlineKeyboardButton(get_text('profile_button', lang), callback_data="show_profile")])
    else:
        keyboard_buttons.append([InlineKeyboardButton(get_text('auth_button', lang), callback_data="start_auth")])
        
    if user_role['isAdmin']:
        keyboard_buttons.append([InlineKeyboardButton(get_text('manage_button', lang), callback_data="manage_menu")])

    if REPORTS_GROUP_URL:
         keyboard_buttons.append([InlineKeyboardButton(get_text('reports_group_button', lang), url=REPORTS_GROUP_URL)])
    if any(user_role.values()):
        keyboard_buttons.append([InlineKeyboardButton(get_text('change_language_button', lang), callback_data="select_language")])

    keyboard = InlineKeyboardMarkup(keyboard_buttons)
    
   
    text = f"*{get_text('main_menu_title', lang)}*" 
    if greeting:
        text = f"{greeting}\n\n{text}"
    
    text += roster_summary_text
    
    # Отправка или редактирование сообщения
    try:
        if message_id_to_edit:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id_to_edit, text=text, reply_markup=keyboard, parse_mode='Markdown')
        else:
            sent_message = await context.bot.send_message(chat_id, text, reply_markup=keyboard, parse_mode='Markdown')
            context.user_data['main_menu_message_id'] = sent_message.message_id
            
    except Exception as e:
        logger.error(f"Ошибка в show_main_menu_logic: {e}. Пробую отправить новое сообщение.")
        sent_message = await context.bot.send_message(chat_id, text, reply_markup=keyboard, parse_mode='Markdown')
        context.user_data['main_menu_message_id'] = sent_message.message_id

async def force_user_to_main_menu(context: ContextTypes.DEFAULT_TYPE, user_id: str, greeting: str, message_to_delete_id: int = None):
    """
    Принудительно отправляет пользователю новое главное меню, удаляя старое сообщение, если нужно.
    """
    try:
       
        if int(user_id) in context._application.user_data:
            context._application.user_data[int(user_id)].clear()
            logger.info(f"Очищены context.user_data для {user_id}")
        
        if message_to_delete_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=message_to_delete_id)
                logger.info(f"Удалено старое сообщение {message_to_delete_id} для {user_id}")
            except Exception as e:
                logger.warning(f"Не удалось удалить старое сообщение {message_to_delete_id} для {user_id}: {e}")

        await show_main_menu_logic(context, user_id, int(user_id), greeting=greeting)
        logger.info(f"Пользователю {user_id} было принудительно показано главное меню.")
    except Exception as e:
        logger.error(f"Не удалось принудительно обновить меню для {user_id}: {e}")

async def back_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Возвращает пользователя в главное меню, редактируя текущее сообщение."""
    query = update.callback_query
    await query.answer()
    
    await show_main_menu_logic(
        context=context,
        user_id=str(query.from_user.id),
        chat_id=query.message.chat_id,
        message_id_to_edit=query.message.message_id
    )

# --- МЕНЮ УПРАВЛЕНИЯ ---
async def manage_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает подменю для администрирования """
    query = update.callback_query
    await query.answer()
    
    user_id = str(query.from_user.id)
    user_role = check_user_role(user_id)

    keyboard = [
        [InlineKeyboardButton("👥 Управление пользователями", callback_data="manage_users")],
        [InlineKeyboardButton("📂 Управление справочниками", callback_data="manage_directories")],
    ]

    if user_role.get('isAdmin'):
         keyboard.append([InlineKeyboardButton("🗂️ Управление отчетами", callback_data="admin_report_menu_start")])

    if user_id == OWNER_ID:
        keyboard.append([InlineKeyboardButton("🗄️ Управление данными", callback_data="manage_db")])

    keyboard.append([InlineKeyboardButton("◀️ Назад в главное меню", callback_data="go_back_to_main_menu")])

    await query.edit_message_text(
        text="⚙️ *Меню управления*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    
# --- МЕНЮ ДЛЯ СПРАВОЧНИКОВ и ВЫГРУЗКИ БД ---
async def manage_db_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает меню управления базой данных (только для Овнера)."""
    query = update.callback_query
    await query.answer()

    keyboard = [
        [InlineKeyboardButton("📥 Скачать резервную копию БД", callback_data="db_backup_download")],
        [InlineKeyboardButton("📤 Загрузить резервную копию БД", callback_data="db_backup_upload_prompt")],
        [InlineKeyboardButton("📋 Список всех пользователей", callback_data="db_export_all_users")],
        [InlineKeyboardButton("◀️ Назад в Управление", callback_data="manage_menu")],
    ]
    text = (
        "🗄️ *Управление данными*\n\n"
        "**ВНИМАНИЕ:** Загрузка резервной копии полностью перезапишет все текущие данные в боте."
    )
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def manage_directories_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает меню для работы со справочниками."""
    query = update.callback_query
    await query.answer()

    keyboard = [
        [InlineKeyboardButton("📄 Скачать шаблон (Excel)", callback_data="get_directories_template_button")],
        [InlineKeyboardButton("◀️ Назад в управление", callback_data="manage_menu")]
    ]
    caption = (
        "Здесь вы можете управлять справочниками:\n\n"
        "1.  **Скачайте шаблон**, чтобы увидеть текущие данные.\n"
        "2.  **Отредактируйте** его (добавьте или измените строки).\n"
        "3.  **Отправьте файл** обратно боту, чтобы применить изменения."
    )

    await query.edit_message_text(
        text=caption,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

# --- БЭКАП и ЛИСТ ПОЛЬЗОВАТЕЛЕЙ---
async def download_db_backup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Формирует и отправляет Овнеру полный бэкап БД в Excel."""
    query = update.callback_query
    await query.answer()
    
    if str(query.from_user.id) != OWNER_ID: return

    await query.edit_message_text("⏳ Формирую полную резервную копию... Это может занять некоторое время.")
    
    file_path = os.path.join(TEMP_DIR, f"full_backup_{date.today()}.xlsx")
    
    try:
        table_names = table_names = ALL_TABLE_NAMES_FOR_BACKUP
        engine = create_engine(DATABASE_URL)
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            with engine.connect() as connection:
                for table_name in table_names:
                    query_check_table = text("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = :table_name)")
                    if connection.execute(query_check_table, {'table_name': table_name}).scalar():
                        df = pd.read_sql_query(text(f"SELECT * FROM {table_name}"), connection)

                        if table_name == 'reports':
                            timezone_cols = ['timestamp', 'kiok_approval_timestamp']
                            for col in timezone_cols:
                                if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
                                    if df[col].dt.tz is not None:
                                        df[col] = df[col].dt.tz_localize(None)
                        
                        df.to_excel(writer, sheet_name=table_name, index=False)
                    else:
                        logger.warning(f"Таблица {table_name} не найдена в БД, пропущена в бэкапе.")
        
        await context.bot.send_document(
            chat_id=OWNER_ID,
            document=open(file_path, 'rb'),
            caption="✅ Полная резервная копия базы данных."
        )
        await query.delete_message()
    except Exception as e:
        logger.error(f"Ошибка при создании бэкапа: {e}")
        await query.message.reply_text("❌ Произошла ошибка при создании резервной копии.")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

async def export_all_users_to_excel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Формирует и отправляет Овнеру единый список всех пользователей."""
    query = update.callback_query
    await query.answer()
    if str(query.from_user.id) != OWNER_ID: return

    await query.edit_message_text("👥 Собираю всех пользователей в один список...")
    file_path = os.path.join(TEMP_DIR, f"all_users_{date.today()}.xlsx")

    try:
        engine = create_engine(DATABASE_URL)
        all_users_df = pd.DataFrame()
        roles = ['admins', 'managers', 'brigades', 'pto', 'kiok']
        
        with engine.connect() as connection:
            for role in roles:
                query_check_table = text("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = :table_name)")
                if connection.execute(query_check_table, {'table_name': role}).scalar():
                    df = pd.read_sql_query(text(f"SELECT user_id, first_name, last_name, phone_number FROM {role}"), connection)
                    df['role'] = role
                    all_users_df = pd.concat([all_users_df, df], ignore_index=True)

        all_users_df.to_excel(file_path, index=False)
        
        await context.bot.send_document(
            chat_id=OWNER_ID,
            document=open(file_path, 'rb'),
            caption="✅ Полный список зарегистрированных пользователей."
        )
        await query.delete_message()
    except Exception as e:
        logger.error(f"Ошибка при экспорте пользователей: {e}")
        await query.message.reply_text("❌ Произошла ошибка при экспорте пользователей.")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

async def daily_backup() -> None:
    """Создает ежедневную резервную копию БД PostgreSQL."""
    logger.info("Начинаю плановое резервное копирование базы данных...")
    backup_filename = f"backup_{date.today().strftime('%Y-%m-%d')}.xlsx"
    file_path = os.path.join(BACKUP_DIR, backup_filename)
    
    try:
        table_names = table_names = ALL_TABLE_NAMES_FOR_BACKUP
        engine = create_engine(DATABASE_URL)
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            with engine.connect() as connection:
                for table_name in table_names:
                    df = pd.read_sql_query(text(f"SELECT * FROM {table_name}"), connection)

                    if table_name == 'reports':
                        timezone_cols = ['timestamp', 'kiok_approval_timestamp']
                        for col in timezone_cols:
                            if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
                                if df[col].dt.tz is not None:
                                    df[col] = df[col].dt.tz_localize(None)

                    df.to_excel(writer, sheet_name=table_name, index=False)

        logger.info(f"Резервная копия успешно создана: {file_path}")
    except Exception as e:
        logger.error(f"Ошибка при создании ежедневного бэкапа: {e}")
        return

    # Очистка старых бэкапов
    try:
        now = datetime.now()
        retention_period = timedelta(days=BACKUP_RETENTION_DAYS)
        
        for filename in os.listdir(BACKUP_DIR):
            file_path_to_check = os.path.join(BACKUP_DIR, filename)
            if os.path.isfile(file_path_to_check):
                file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path_to_check))
                if (now - file_mod_time) > retention_period:
                    os.remove(file_path_to_check)
                    logger.info(f"Удален старый бэкап: {filename}")
    except Exception as e:
        logger.error(f"Ошибка при очистке старых бэкапов: {e}")

async def post_init(application: Application) -> None:
    """
    Запускает планировщик после полной инициализации бота.
    """
    scheduler = AsyncIOScheduler(timezone='Asia/Tashkent')
    scheduler.add_job(daily_backup, 'cron', hour=3, minute=0)
    scheduler.start()
    application.bot_data["scheduler"] = scheduler
    logger.info("Планировщик для ежедневных бэкапов запущен через post_init.")

async def post_stop(application: Application) -> None:
    """
    Корректно останавливает планировщик перед завершением работы бота.
    """
    if application.bot_data.get("scheduler"):
        application.bot_data["scheduler"].shutdown()
        logger.info("Планировщик остановлен через pre_stop.")


# --- ДИАЛОГ БЭКАП ---
async def prompt_for_restore_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Просит пользователя отправить файл для восстановления."""
    query = update.callback_query
    await query.answer()
    text = (
        "**⚠️ ВНИМАНИЕ! ⚠️**\n"
        "Следующий отправленный Excel-файл будет использован для **полного восстановления базы данных**. "
        "Все текущие данные будут стерты.\n\n"
        "Для продолжения, **отправьте файл**.\n"
        "Для отмены нажмите /cancel."
    )
    await query.edit_message_text(text, parse_mode="Markdown")
    return AWAITING_RESTORE_FILE

async def handle_db_restore_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает загруженный Excel-файл для восстановления БД и обновляет счетчики ID."""
    await update.message.reply_text("✅ Файл получен. Начинаю процесс восстановления. Бот может не отвечать некоторое время...")
    
    file = await context.bot.get_file(update.message.document.file_id)
    file_path = os.path.join(TEMP_DIR, "restore_db.xlsx")
    await file.download_to_drive(file_path)

    table_order = [
        'disciplines', 'construction_objects', 'work_types', 'admins', 
        'managers', 'brigades', 'pto', 'kiok', 'reports', 'topic_mappings', 'personnel_roles', 'daily_rosters', 'daily_roster_details'
    ]
    
    serial_pk_tables = ['disciplines', 'construction_objects', 'work_types', 'reports', 'personnel_roles', 'daily_rosters', 'daily_roster_details']

    engine = create_engine(DATABASE_URL)
    xls = None
    try:
        xls = pd.ExcelFile(file_path)
        with engine.connect() as connection:
            with connection.begin() as transaction:
                for table_name in reversed(table_order):
                    connection.execute(text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;'))
                
                for table_name in table_order:
                    if table_name in xls.sheet_names:
                        df = pd.read_excel(xls, sheet_name=table_name)
                        df.dropna(how='all', inplace=True)
                        if not df.empty:
                            df.to_sql(table_name, con=connection, if_exists='append', index=False)
                            logger.info(f"Таблица {table_name} успешно восстановлена.")

                logger.info("Обновление счетчиков последовательностей (sequences)...")
                for table_name in serial_pk_tables:
                    
                    update_seq_query = text(f"SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), COALESCE((SELECT MAX(id) FROM {table_name}), 1));")
                    connection.execute(update_seq_query)
                    logger.info(f"Счетчик для таблицы '{table_name}' обновлен.")
                

        await update.message.reply_text("✅✅✅ **Восстановление базы данных успешно завершено!**")

    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА при восстановлении БД: {e}")
        await update.message.reply_text(f"❌❌❌ **ОШИБКА!** Восстановление было отменено: {e}")
    finally:
        if xls: xls.close()
        if os.path.exists(file_path): os.remove(file_path)
            
    return ConversationHandler.END

async def cancel_restore(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет процесс восстановления БД."""
    await update.message.reply_text("Операция восстановления отменена.")
    return ConversationHandler.END

# --- ЛОГИКА Формирования отчетов ---

async def start_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Начинает диалог создания отчета .
    """
    query = update.callback_query
    await query.answer()

    user_id = str(query.from_user.id)
    user_role = check_user_role(user_id)
    
    lang = get_user_language(user_id)

    if user_role.get('isAdmin') or user_role.get('managerLevel') == 1:
        disciplines = db_query("SELECT name FROM disciplines ORDER BY name")
        if not disciplines:
            await query.edit_message_text("⚠️ В базе данных нет дисциплин, невозможно создать отчет.")
            return ConversationHandler.END

        keyboard = [[InlineKeyboardButton(get_data_translation(name, lang), callback_data=f"owner_select_disc_{name}")] for name, in disciplines]
        keyboard.append([InlineKeyboardButton(get_text('cancel_button', lang), callback_data="cancel_report")])
        
        await query.edit_message_text(
            text=f"*{get_text('report_step1_discipline', lang)}*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return OWNER_SELECTING_DISCIPLINE

    else:
        context.user_data['report_data'] = {'discipline_name': user_role.get('discipline')}
        await show_corps_page(update, context, page=1)
        return GETTING_CORPUS

async def show_corps_page(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int):
    """Отображает указанную страницу корпусов ."""
    
    chat_id = update.effective_chat.id
    message_id_to_edit = update.callback_query.message.message_id if update.callback_query else None
    user_id = str(update.effective_user.id)
    lang = get_user_language(user_id)

    corps_list_raw = db_query("SELECT id, name FROM construction_objects ORDER BY display_order ASC, name ASC")
    
    if not corps_list_raw:
        
        text = "⚠️ *Ошибка:* Не удалось найти ни одного корпуса в базе данных. Обратитесь к администратору."
        if message_id_to_edit:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id_to_edit, text=text, parse_mode='Markdown')
        else:
            await context.bot.send_message(chat_id, text, parse_mode='Markdown')
        return ConversationHandler.END

    total_corps = len(corps_list_raw)
    total_pages = math.ceil(total_corps / ELEMENTS_PER_PAGE) if total_corps > 0 else 1

    start_index = (page - 1) * ELEMENTS_PER_PAGE
    end_index = start_index + ELEMENTS_PER_PAGE
    corps_on_page = corps_list_raw[start_index:end_index]

    keyboard_buttons = []
    
    for corps_id, corps_name in corps_on_page:
        keyboard_buttons.append([InlineKeyboardButton(corps_name, callback_data=f"report_corp_{corps_id}")])

    navigation_buttons = []
    if page > 1:
        navigation_buttons.append(InlineKeyboardButton(get_text('back_button', lang), callback_data=f"paginate_corps_{page - 1}"))
    if page < total_pages:
        navigation_buttons.append(InlineKeyboardButton(get_text('forward_button', lang), callback_data=f"paginate_corps_{page + 1}"))
    if navigation_buttons:
        keyboard_buttons.append(navigation_buttons)

    keyboard_buttons.append([InlineKeyboardButton(get_text('cancel_button', lang), callback_data="cancel_report")])
    keyboard = InlineKeyboardMarkup(keyboard_buttons)

    text = f"*{get_text('report_step1_corpus', lang)}* ({get_text('page_of', lang).format(page=page, total_pages=total_pages)})"
    
    if message_id_to_edit:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id_to_edit,
            text=text,
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
    else:
        await context.bot.send_message(
            chat_id, 
            text, 
            reply_markup=keyboard, 
            parse_mode='Markdown'
        )

async def cancel_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет процесс создания отчета и возвращает в главное меню."""
    query = update.callback_query
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)

    await query.answer(get_text('report_creation_cancelled', lang))

    await show_main_menu_logic(
        context=context,
        user_id=user_id,
        chat_id=query.message.chat_id,
        message_id_to_edit=query.message.message_id
    )
    
    context.user_data.clear()
    return ConversationHandler.END

async def go_back_in_report_creation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Централизованно обрабатывает навигацию 'назад' в диалоге создания отчета."""
    query = update.callback_query
    await query.answer()
    
    step_to_return_to = query.data.split('_', 2)[2] 

    if step_to_return_to == 'start_report':
        await show_corps_page(update, context, page=1)
        return GETTING_CORPUS

    elif step_to_return_to == 'ask_work_type':
        await show_work_types_page(update, context, page=1)
        return GETTING_WORK_TYPE

    await query.message.delete() 
    chat_id = query.message.chat_id

    if step_to_return_to == 'ask_count':
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="back_to_ask_work_type")]]
        sent_message = await context.bot.send_message(
            chat_id, "📝 *Шаг 3: Укажите количество человек на объекте*",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown'
        )
        context.user_data['last_bot_message_id'] = sent_message.message_id
        return GETTING_PEOPLE_COUNT

    elif step_to_return_to == 'ask_volume':
        unit_of_measure = context.user_data['report_data'].get('unit_of_measure', '')
        volume_prompt = "📝 *Шаг 4: Укажите выполненный объем*"
        if unit_of_measure:
            volume_prompt += f" *в {unit_of_measure}*:"
        else:
            volume_prompt += ":"

        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="back_to_ask_count")]]
        sent_message = await context.bot.send_message(
            chat_id, volume_prompt,
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown'
        )
        context.user_data['last_bot_message_id'] = sent_message.message_id
        return GETTING_VOLUME
    
    elif step_to_return_to == 'ask_date':
        keyboard = [
            [InlineKeyboardButton("◀️ Назад", callback_data="back_to_ask_volume")],
            [InlineKeyboardButton("Сегодня", callback_data="set_date_today"), InlineKeyboardButton("Вчера", callback_data="set_date_yesterday")]
        ]
        text = "📝 *Шаг 5: Выберите дату или введите ее вручную (ДД.ММ.ГГГГ)*"
        sent_message = await context.bot.send_message(
            chat_id, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        context.user_data['last_bot_message_id'] = sent_message.message_id
        return GETTING_DATE

    return ConversationHandler.END

async def owner_select_discipline_and_ask_corpus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет дисциплину, выбранную админом, и запрашивает корпус."""
    query = update.callback_query
    await query.answer()

    discipline_name = query.data.split('_', 3)[-1]
    context.user_data['report_data'] = {'discipline_name': discipline_name}

    await show_corps_page(update, context, page=1)
    return GETTING_CORPUS

# --- Отчет для руководителя---

async def report_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает динамическую сводку-дашборд """
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)
    
    try:
        await query.delete_message()
    except Exception as e:
        logger.warning(f"Не удалось удалить сообщение в report_menu: {e}")
    
    wait_msg = await context.bot.send_message(chat_id, get_text('loading_please_wait', lang))
    
    user_role = check_user_role(user_id)
    
    parts = query.data.split('_')
    period = parts[2] if len(parts) > 2 else 'all'

    date_filter_sql = ""
    date_params = []

    target_date_for_summary = date.today()
    period_text = get_text('all_time_button', lang)
    
    if period == 'today':
        target_date_for_summary = date.today()
        date_filter_sql = "AND report_date = %s"
        date_params.append(target_date_for_summary.strftime('%Y-%m-%d'))
        period_text = get_text('today_button', lang)
    elif period == 'yesterday':
        target_date_for_summary = date.today() - timedelta(days=1)
        date_filter_sql = "AND report_date = %s"
        date_params.append(target_date_for_summary.strftime('%Y-%m-%d'))
        period_text = get_text('yesterday_button', lang)
    
    try:
        if user_role.get('isAdmin') or user_role.get('managerLevel') == 1:
            brigade_details_query = """
                SELECT 
                    d.name,
                    (SELECT COUNT(*) FROM brigades WHERE discipline = d.id) as total_brigades,
                    (SELECT COUNT(DISTINCT b.user_id) FROM brigades b JOIN reports r ON b.brigade_name = r.foreman_name WHERE b.discipline = d.id AND r.report_date = %s) as reported_count
                FROM disciplines d
                ORDER BY d.name;
            """
            date_for_header = date.today() if period == 'all' else target_date_for_summary
            brigade_counts_raw = db_query(brigade_details_query, (date_for_header,))
            
            total_brigades = sum(total for _, total, _ in brigade_counts_raw) if brigade_counts_raw else 0
            
            brigade_details_lines = []
            if brigade_counts_raw:

                for disc_name, total, reported in brigade_counts_raw:
                    if total > 0:
                        line = f"    - {get_data_translation(disc_name, lang)}: *{total}* (сдали отчет: *{reported}*)"
                        brigade_details_lines.append(line)
            
            message_text_intro = (
                f"📊 *{get_text('report_menu_summary_title', lang).format(period=period_text)}*\n\n"
                f"▪️ {get_text('total_brigades_in_system', lang)} *{total_brigades}*\n"
                + "\n".join(brigade_details_lines)
            )

            final_params = tuple(date_params)
            role_filter_sql = ""

        elif user_role.get('isForeman'):
             brigade_name = user_role.get('brigadeName')
             role_filter_sql = "AND foreman_name = %s"
             final_params = (brigade_name,) + tuple(date_params)
             message_text_intro = f"📊 *{get_text('summary_for_your_brigade', lang).format(period=period_text)}*\n\n"
        
        else: 
            discipline_name = user_role.get('discipline')
            if not discipline_name:
                raise ValueError("Дисциплина не найдена для этой роли.")
                
            discipline_id_raw = db_query("SELECT id FROM disciplines WHERE name = %s", (discipline_name,))
            discipline_id = discipline_id_raw[0][0] if discipline_id_raw else None
            
            total_brigades_raw = db_query("SELECT COUNT(*) FROM brigades WHERE discipline = %s", (discipline_id,))
            total_brigades = total_brigades_raw[0][0] if total_brigades_raw else 0
            
            role_filter_sql = "AND discipline_name = %s"
            final_params = (discipline_name,) + tuple(date_params)
            message_text_intro = (
                f"📊 *Сводка по дисциплине «{get_data_translation(discipline_name, lang)}» ({period_text}):*\n\n"
                f"▪️ {get_text('brigades_in_discipline', lang)} *{total_brigades}*\n"
            )

        status_query = f"SELECT kiok_approved, COUNT(*) FROM reports WHERE 1=1 {role_filter_sql} {date_filter_sql} GROUP BY kiok_approved"
        status_counts_raw = db_query(status_query, final_params)
        
        status_counts = {row[0]: row[1] for row in status_counts_raw} if status_counts_raw else {}
        total_reports = sum(status_counts.values())
        approved = status_counts.get(1, 0)
        rejected = status_counts.get(-1, 0)
        pending = status_counts.get(0, 0)

        message_text = (
            message_text_intro + "\n\n" +
            f"▪️ {get_text('reports_for_period', lang)} *{total_reports}*\n"
            f"    - {get_text('reports_approved', lang)} *{approved}*\n"
            f"    - {get_text('reports_rejected', lang)} *{rejected}*\n"
            f"    - {get_text('reports_pending', lang)} *{pending}*\n\n"
            f"*{get_text('select_report_detail', lang)}*"
        )

    except Exception as e:
        logger.error(f"Ошибка при сборе статистики для report_menu: {e}")
        message_text = f"❗*{get_text('error_generic', lang)}*"
    
    time_filter_buttons = [
        InlineKeyboardButton(get_text('yesterday_button', lang), callback_data="report_menu_yesterday"),
        InlineKeyboardButton(get_text('today_button', lang), callback_data="report_menu_today"),
        InlineKeyboardButton(get_text('all_time_button', lang), callback_data="report_menu_all"),
    ]
    
    dashboard_buttons = []
    if user_role.get('isForeman'):
         dashboard_buttons.append([InlineKeyboardButton(get_text('foreman_performance_button', lang), callback_data="foreman_performance")])
    else:
        if (user_role.get('isManager') or user_role.get('isPto') or user_role.get('isAdmin')) and not user_role.get('isKiok'):
          dashboard_buttons.append([InlineKeyboardButton(get_text('report_overview_button', lang), callback_data="report_overview")])
        
        if user_role.get('isManager') or user_role.get('isPto'):
          dashboard_buttons.append([InlineKeyboardButton(get_text('problem_brigades_button', lang), callback_data="handle_problem_brigades_button")])
        if user_role.get('isManager'):
          dashboard_buttons.append([InlineKeyboardButton(get_text('historical_overview_button', lang), callback_data="report_historical")])
        if user_role.get('isPto') or user_role.get('isKiok') or user_role.get('isAdmin'):
             dashboard_buttons.append([InlineKeyboardButton(get_text('export_excel_button', lang), callback_data="get_excel_report")])

        if user_role.get('isManager') or user_role.get('isAdmin') or user_role.get('isPto') or user_role.get('isKiok'):
             dashboard_buttons.append([InlineKeyboardButton(get_text('hr_menu_button', lang), callback_data="hr_menu")])
        
        if user_role.get('isAdmin'):
             dashboard_buttons.append([InlineKeyboardButton(get_text('delete_report_button', lang), callback_data="delete_report_list_1")])

    dashboard_buttons.append([InlineKeyboardButton(get_text('back_to_main_menu_button', lang), callback_data="go_back_to_main_menu")])
    
    keyboard = [time_filter_buttons] + dashboard_buttons
    
    await wait_msg.edit_text(
        text=message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def show_overview_dashboard_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, selected_date_override: date = None) -> int:

    query = update.callback_query

    selected_date = date.today()
    if selected_date_override:
        selected_date = selected_date_override
    elif query:
        await query.answer()
        if query.data.startswith("report_overview_date_"):
            date_str = query.data.split('_')[-1]
            if date_str == 'today':
                selected_date = date.today()
            elif date_str == 'yesterday':
                selected_date = date.today() - timedelta(days=1)
            else:
                try:
                    selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                except ValueError:
                    selected_date = date.today()

    context.user_data['overview_date'] = selected_date.strftime('%Y-%m-%d')
    date_str_for_callback = context.user_data['overview_date']
    
    user_id = str(update.effective_user.id)
    lang = get_user_language(user_id)
    user_role = check_user_role(user_id)

    if query and query.message:
        await query.message.delete()
    elif update.message:
        await update.message.delete()

    wait_msg = await context.bot.send_message(update.effective_chat.id, f"⏳ {get_text('loading_please_wait', lang)}")

    try:
        engine = create_engine(DATABASE_URL)
        
        pd_query = """
            SELECT r.discipline_name, r.work_type_name, r.people_count, r.volume, wt.norm_per_unit, wt.unit_of_measure
            FROM reports r
            LEFT JOIN work_types wt ON r.work_type_name = wt.name AND r.discipline_name = (SELECT d.name FROM disciplines d WHERE d.id = wt.discipline_id)
            WHERE r.report_date = :selected_date
        """
        params = {'selected_date': selected_date.strftime('%Y-%m-%d')}

        if not (user_role.get('isAdmin') or user_role.get('managerLevel') == 1):
            user_discipline = user_role.get('discipline')
            if not user_discipline:
                 await wait_msg.edit_text("Ошибка: для вашей роли не задана дисциплина.")
                 return ConversationHandler.END
            pd_query += " AND r.discipline_name = :discipline"
            params['discipline'] = user_discipline
        
        with engine.connect() as connection:
            df = pd.read_sql_query(text(pd_query), connection, params=params)

        date_text = selected_date.strftime('%d.%m.%Y')
        title = get_text('overview_summary_title', lang) + f" на {date_text}"

        if df.empty:
            message_text = f"*{title}*\n\n_{get_text('no_reports_for_period', lang)}_"
        else:
            df['planned_volume'] = df['people_count'] * df['norm_per_unit'].fillna(0)
            other_works_df = df[df['norm_per_unit'].isnull()]
            main_df = df[df['norm_per_unit'].notnull()]

            message_lines = [f"📊 *{title}*"]
            all_disciplines = df['discipline_name'].unique()

            for discipline in all_disciplines:
                disc_df = main_df[main_df['discipline_name'] == discipline]
                disc_other_df = other_works_df[other_works_df['discipline_name'] == discipline]
                if disc_df.empty and disc_other_df.empty: continue

                total_people = int(disc_df['people_count'].sum() + disc_other_df['people_count'].sum())
                
                if not disc_df.empty:
                    work_summary = disc_df.groupby('work_type_name').agg(
                        total_volume=('volume', 'sum'),
                        total_planned=('planned_volume', 'sum'),
                        total_people=('people_count', 'sum'),
                        unit=('unit_of_measure', 'first')
                    ).reset_index()
                    work_summary['performance'] = (work_summary['total_volume'] / work_summary['total_planned']) * 100
                    
                    avg_performance = (work_summary['total_volume'].sum() / work_summary['total_planned'].sum() * 100) if work_summary['total_planned'].sum() > 0 else 0
                else:
                    work_summary = pd.DataFrame()
                    avg_performance = 0

                message_lines.append("\n" + "─" * 15)
                message_lines.append(f"*{get_data_translation(discipline, lang)}*")
                message_lines.append(f"▪️ {get_text('total_people_short', lang)}: *{total_people} чел.*")
                message_lines.append(f"▪️ {get_text('average_output_short', lang)}: *{avg_performance:.1f}%*")

                if not work_summary.empty:
                    message_lines.append(f"_{get_text('work_details_label', lang)}_")
                    for _, row in work_summary.iterrows():
                        unit_str = row['unit'] or ''
                        fact_volume_str = f"| {row['total_volume']:.1f} {unit_str}"
                        message_lines.append(f"  - {get_data_translation(row['work_type_name'], lang)}: *{int(row['total_people'])} чел.* ({row['performance']:.1f}%) {fact_volume_str}")

                if not disc_other_df.empty:
                    other_people_count = int(disc_other_df['people_count'].sum())
                    message_lines.append(f"_{get_text('other_works_label', lang)}:_ *{other_people_count} чел.*")
            message_text = "\n".join(message_lines)
        
        date_buttons = [
            InlineKeyboardButton("Вчера", callback_data="report_overview_date_yesterday"),
            InlineKeyboardButton("Сегодня", callback_data="report_overview_date_today"),
            InlineKeyboardButton("Выбрать дату", callback_data="report_overview_pick_date")
        ]
        
        keyboard_buttons = [date_buttons]

        if user_role.get('isAdmin') or user_role.get('managerLevel') == 1:
           disciplines = db_query("SELECT id, name FROM disciplines ORDER BY name")
           if disciplines:
              for disc_id, disc_name in disciplines:
                 keyboard_buttons.append([InlineKeyboardButton(f"{get_data_translation(disc_name, lang)}", callback_data=f"gen_overview_chart_{disc_id}_{date_str_for_callback}")])

        elif user_role.get('isPto') or user_role.get('managerLevel') == 2:
           user_discipline_name = user_role.get('discipline')
           if user_discipline_name:
             discipline_id_raw = db_query("SELECT id FROM disciplines WHERE name = %s", (user_discipline_name,))
             if discipline_id_raw:
                user_discipline_id = discipline_id_raw[0][0]
                keyboard_buttons.append([InlineKeyboardButton("📊 Показать мой график", callback_data=f"gen_overview_chart_{user_discipline_id}_{date_str_for_callback}")])

        keyboard_buttons.append([InlineKeyboardButton("◀️ Назад в меню отчетов", callback_data="report_menu_all")])

        await wait_msg.edit_text(text=message_text, reply_markup=InlineKeyboardMarkup(keyboard_buttons), parse_mode="Markdown")
        return SELECTING_OVERVIEW_ACTION

    except Exception as e:
        logger.error(f"Ошибка в show_overview_dashboard_menu: {e}")
        await wait_msg.edit_text(f"❗ *{get_text('error_generic', lang)}*")
        return ConversationHandler.END

async def generate_overview_chart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:

    query = update.callback_query
    await query.answer()

    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)

    try:
        base_callback, date_str = query.data.rsplit('_', 1)
        discipline_id = int(base_callback.split('_')[-1])
        selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except (ValueError, IndexError):
        logger.error(f"Критическая ошибка разбора callback_data в generate_overview_chart: {query.data}")
        await query.edit_message_text("❌ Произошла внутренняя ошибка. Не удалось разобрать данные для графика.")
        return SELECTING_OVERVIEW_ACTION

    discipline_name_raw = db_query("SELECT name FROM disciplines WHERE id = %s", (discipline_id,))
    if not discipline_name_raw:
        await query.edit_message_text("❌ Ошибка: Дисциплина с таким ID не найдена.")
        return SELECTING_OVERVIEW_ACTION
    discipline_name = discipline_name_raw[0][0]
    
    context.user_data['overview_date'] = date_str

    if query.message:
        await query.edit_message_text(f"⏳ {get_text('loading_please_wait', lang)}")
    
    chart_path = None

    try:
        engine = create_engine(DATABASE_URL)
        
        pd_query = """
            SELECT r.work_type_name, r.people_count, r.volume, wt.norm_per_unit
            FROM reports r
            LEFT JOIN work_types wt ON r.work_type_name = wt.name AND r.discipline_name = (SELECT d.name FROM disciplines d WHERE d.id = wt.discipline_id)
            WHERE r.report_date = :report_date AND r.discipline_name = :discipline
        """
        params = {'report_date': date_str, 'discipline': discipline_name}

        with engine.connect() as connection:
            df = pd.read_sql_query(text(pd_query), connection, params=params)

        if df.empty or df['norm_per_unit'].isnull().all():
            await query.edit_message_text(
                f"Нет данных для построения графика\n\nПо дисциплине «{discipline_name}» за выбранный период нет отчетов с нормируемыми работами.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data=f"report_overview_date_{date_str}")]]),
            )
            return SELECTING_OVERVIEW_ACTION

        df['is_prochie'] = df['work_type_name'].str.contains('Прочие', case=False, na=False) | df['norm_per_unit'].isnull()
        prochie_df = df[df['is_prochie'] == True]
        main_df = df[df['is_prochie'] == False].copy()
        prochie_people_count = int(prochie_df['people_count'].sum())

        if main_df.empty:
            await query.edit_message_text(
                f"Нет данных для построения графика\n\nНа дату {selected_date.strftime('%d.%m.%Y')} есть только 'Прочие работы', для которых график не строится.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data=f"report_overview_date_{date_str}")]]),
            )
            return SELECTING_OVERVIEW_ACTION

        main_df['plan'] = main_df['people_count'] * main_df['norm_per_unit']
        df_chart = main_df.groupby('work_type_name').agg(
            План=('plan', 'sum'), 
            Факт=('volume', 'sum'),
            Люди=('people_count', 'sum')
        ).reset_index()

        df_chart['x_label'] = df_chart.apply(
            lambda row: f"{row['work_type_name']}\n({int(row['Люди'])} чел.)", axis=1
        )

        plt.style.use('seaborn-v0_8-darkgrid')
        fig, ax = plt.subplots(figsize=(12, 8))
        df_chart.plot(x='x_label', y=['План', 'Факт'], kind='bar', ax=ax, width=0.7, legend=True)
        
        for bar in ax.patches:
            ax.annotate(f'{bar.get_height():.1f}',
                        (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        ha='center', va='center', size=9, xytext=(0, 8),
                        textcoords='offset points', fontweight='bold')

        ax.set_title(f"Выработка по дисциплине «{discipline_name}» за {selected_date.strftime('%d.%m.%Y')}", fontsize=16, fontweight='bold')
        ax.set_ylabel("Объем работ", fontsize=12)
        ax.set_xlabel("")
        ax.tick_params(axis='x', labelsize=10, rotation=15)
        ax.legend(fontsize=12)
        ax.set_ylim(top=ax.get_ylim()[1] * 1.15)
        plt.tight_layout(pad=2.0)
        
        chart_path = os.path.join(DASHBOARD_DIR, f'chart_{user_id}.png')
        plt.savefig(chart_path)
        plt.close(fig)

        safe_discipline_name = escape_markdown(get_data_translation(discipline_name, lang), version=2)
        caption_text = f"*Анализ выработки для дисциплины «{safe_discipline_name}»*"
        if prochie_people_count > 0:
            caption_text += f"\n\n*Примечание:* на прочих работах было задействовано *{prochie_people_count}* чел\."
        
        await query.message.delete()
        
        with open(chart_path, 'rb') as chart_file:
            await context.bot.send_photo(
                chat_id=query.message.chat_id,
                photo=chart_file,
                caption=caption_text,
                parse_mode='MarkdownV2',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Назад к сводке", callback_data=f"report_overview_date_{date_str}")
                ]])
            )

    except Exception as e:
        logger.error(f"Ошибка при создании или отправке графика: {e}")
        if query and query.message:
            # ИЗМЕНЕНИЕ №3: Заменен текст
            await query.message.edit_text("❌ Произошла ошибка при создании графика.")
    finally:
        if chart_path and os.path.exists(chart_path):
            os.remove(chart_path)
            
    return SELECTING_OVERVIEW_ACTION

async def report_overview_chart_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
 
    """Запрашивает дисциплину для построения графика и отображает кнопки выбора."""
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)
    user_role = check_user_role(user_id)

    date_str = context.user_data.get('overview_date')
    if not date_str:
        await query.edit_message_text("Ошибка: не удалось определить дату. Пожалуйста, начните сначала.")
        return SELECTING_OVERVIEW_ACTION

    keyboard_buttons = []

    if user_role.get('isAdmin') or user_role.get('managerLevel') == 1:
        disciplines = db_query("SELECT name FROM disciplines ORDER BY name")
        if not disciplines:
            await query.edit_message_text("Ошибка: нет дисциплин для построения графика.")
            return SELECTING_OVERVIEW_ACTION
        
        for name, in disciplines:
             keyboard_buttons.append([InlineKeyboardButton(get_data_translation(name, lang), callback_data=f"gen_overview_chart_{name}_{date_str}")])
  
    else:
        user_discipline = user_role.get('discipline')
        if not user_discipline:
            await query.edit_message_text("Ошибка: для вашей роли не задана дисциплина.")
            return SELECTING_OVERVIEW_ACTION
        keyboard_buttons.append([InlineKeyboardButton(get_data_translation(user_discipline, lang), callback_data=f"gen_overview_chart_{user_discipline}_{date_str}")])
    
    keyboard_buttons.append([InlineKeyboardButton("◀️ Назад", callback_data=f"report_overview_date_{date_str}")])

    await query.edit_message_text(
        "*Выберите дисциплину для просмотра графика:*",
        reply_markup=InlineKeyboardMarkup(keyboard_buttons),
        parse_mode="Markdown"
    )

    return SELECTING_OVERVIEW_ACTION

async def prompt_for_overview_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запрашивает дату и сохраняет ID сообщения с запросом."""
    query = update.callback_query
    await query.answer()
    date_str = context.user_data.get('overview_date', date.today().strftime('%Y-%m-%d'))
    keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"report_overview_date_{date_str}")]]

    if query.message:
        await query.message.delete()

    sent_message = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Введите дату в формате *ДД.ММ.ГГГГ*:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    context.user_data['last_prompt_message_id'] = sent_message.message_id
    
    return AWAITING_OVERVIEW_DATE

async def process_overview_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает ручной ввод, удаляет сообщение с запросом и вызывает основную функцию.
    """

    prompt_message_id = context.user_data.pop('last_prompt_message_id', None)
    if prompt_message_id:
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=prompt_message_id)
        except Exception as e:
            logger.info(f"Не удалось удалить сообщение с запросом даты: {e}")

    try:
        selected_date = datetime.strptime(update.message.text, "%d.%m.%Y").date()
 
        return await show_overview_dashboard_menu(update, context, selected_date_override=selected_date)
    except (ValueError, AttributeError):
        await update.message.reply_text("Неверный формат даты. Попробуйте еще раз.")
        return AWAITING_OVERVIEW_DATE

async def show_historical_report_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Главный обработчик для "Исторического обзора".
    Проверяет роль и показывает либо общую сводку с выбором (для админов),
    либо детальный отчет по конкретной дисциплине (для остальных).
    """
    query = update.callback_query
    await query.answer()

    user_role = check_user_role(str(query.from_user.id))

    if user_role.get('isAdmin') or user_role.get('managerLevel') == 1:
        await query.edit_message_text("⏳ Собираю общую сводку по всем дисциплинам...")
        
        try:
            header = "📊 *Общая сводка по всем дисциплинам*"
            
            report_stats_raw = db_query("SELECT kiok_approved, COUNT(*) FROM reports GROUP BY kiok_approved")
            report_stats = {str(status): count for status, count in report_stats_raw} if report_stats_raw else {}
            total_reports = sum(report_stats.values())
            
            today_str = date.today().strftime('%Y-%m-%d')
            all_brigades = {row[0] for row in db_query("SELECT brigade_name FROM brigades")}
            reported_today = {row[0] for row in db_query("SELECT DISTINCT foreman_name FROM reports WHERE report_date = %s", (today_str,))}
            non_reporters_count = len(all_brigades - reported_today)

            analysis_lines = ["\n*Данные для анализа отсутствуют.*"]
            analysis_header = "\n📊 *Средняя выработка по дисциплинам:*"
            overall_output_percent = 0

            engine = create_engine(DATABASE_URL)
            pd_query = """
                SELECT r.discipline_name, r.volume, r.people_count, wt.norm_per_unit
                FROM reports r JOIN disciplines d ON r.discipline_name = d.name
                JOIN work_types wt ON d.id = wt.discipline_id AND r.work_type_name = wt.name
            """
            with engine.connect() as connection:
                df = pd.read_sql_query(text(pd_query), connection)

            if not df.empty:
                df['planned_volume'] = pd.to_numeric(df['people_count']) * pd.to_numeric(df['norm_per_unit'])
                df['volume'] = pd.to_numeric(df['volume'])
                overall_output_percent = (df['volume'].sum() / df['planned_volume'].sum()) * 100 if df['planned_volume'].sum() > 0 else 0
                
                discipline_summary = df.groupby('discipline_name').apply(
                    lambda x: (x['volume'].sum() / x['planned_volume'].sum()) * 100 if x['planned_volume'].sum() > 0 else 0
                ).reset_index(name='avg_output')
                
                analysis_lines = [f"  - *{row['discipline_name']}*: средняя выработка *{row['avg_output']:.1f}%*" for _, row in discipline_summary.sort_values(by='avg_output', ascending=False).iterrows()]

            message = [header, "---", f"📈 *Статистика отчетов (за все время):*\n  - Всего подано: *{total_reports}*\n  - ✅ Согласовано: *{report_stats.get('1', 0)}*\n  - ❌ Отклонено: *{report_stats.get('-1', 0)}*\n  - ⏳ Ожидает: *{report_stats.get('0', 0)}*", f"\n🚫 *Не сдали отчет сегодня: {non_reporters_count} бригад*", f"\n💡 *Общая средняя выработка: {overall_output_percent:.1f}%*", analysis_header]
            message.extend(analysis_lines)
            message.append("\n\n🗂️ *Выберите дисциплину для детального отчета:*")
            final_text = "\n".join(message)

            disciplines = db_query("SELECT name FROM disciplines ORDER BY name")
            keyboard_buttons = []
            if disciplines:
                
                lang = get_user_language(str(query.from_user.id))
                for name, in disciplines:
                    
                    translated_discipline = get_data_translation(name, lang)
                    button_text = get_text('detail_by_discipline_button', lang).format(discipline=translated_discipline)
                    keyboard_buttons.append([InlineKeyboardButton(button_text, callback_data=f"gen_hist_report_{name}")])

            keyboard_buttons.append([InlineKeyboardButton(get_text('back_button', lang), callback_data="report_menu_all")])
            await query.edit_message_text(text=final_text, reply_markup=InlineKeyboardMarkup(keyboard_buttons), parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Ошибка при формировании общей сводки: {e}")
            await query.edit_message_text("❌ Произошла ошибка при формировании сводки.")
    else:
        discipline = user_role.get('discipline')
        if not discipline:
            await query.edit_message_text("❗️*Ошибка:* Для вашей роли не задана дисциплина.")
            return
        await generate_discipline_dashboard(update, context, discipline_name=discipline)

async def generate_discipline_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE, discipline_name: str = None) -> None:
    """Собирает всю аналитику по ОДНОЙ дисциплине (ИСПРАВЛЕННАЯ ВЕРСИЯ)."""
    query = update.callback_query
    

    await query.answer()

   
    if not discipline_name:
        if "gen_hist_report_" in query.data:
            discipline_name = query.data.replace('gen_hist_report_', '')
        else:
            
            discipline_name = query.data.split('_', 3)[-1]

    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)
    
    await query.edit_message_text(f"⏳ {get_text('loading_please_wait', lang)} ({get_data_translation(discipline_name, lang)})...", parse_mode="Markdown")
    
    user_role = check_user_role(user_id)

    try:
        header = f"📊 *Подробный отчет по дисциплине «{get_data_translation(discipline_name, lang)}»*"
        params = (discipline_name,)
        
        discipline_id_raw = db_query("SELECT id FROM disciplines WHERE name = %s", params)
        disc_id = discipline_id_raw[0][0] if discipline_id_raw else None
        
        user_counts = {'brigades': 0, 'pto': 0, 'kiok': 0}
        if disc_id:
            for role in user_counts.keys():
                count_raw = db_query(f"SELECT COUNT(*) FROM {role} WHERE discipline = %s", (disc_id,))
                if count_raw: user_counts[role] = count_raw[0][0]
        
        report_stats_raw = db_query("SELECT kiok_approved, COUNT(*) FROM reports WHERE discipline_name = %s GROUP BY kiok_approved", params)
        report_stats = {str(status): count for status, count in report_stats_raw} if report_stats_raw else {}
        total_reports = sum(report_stats.values())
        
        today_str = date.today().strftime('%Y-%m-%d')
        all_brigades_q = db_query("SELECT brigade_name FROM brigades WHERE discipline = %s", (disc_id,)) if disc_id else []
        all_brigades = {row[0] for row in all_brigades_q}
        reported_today = {row[0] for row in db_query("SELECT DISTINCT foreman_name FROM reports WHERE discipline_name = %s AND report_date = %s", params + (today_str,))}
        non_reporters_count = len(all_brigades - reported_today)
        
        low_performance_count = get_low_performance_brigade_count(discipline_name)

        analysis_lines = []
        analysis_header = ""
        overall_output_line = ""

        if not user_role.get('isKiok'):
            engine = create_engine(DATABASE_URL)
            pd_query = """
                SELECT r.work_type_name, r.volume, r.people_count, wt.norm_per_unit 
                FROM reports r JOIN disciplines d ON r.discipline_name = d.name 
                JOIN work_types wt ON d.id = wt.discipline_id AND r.work_type_name = wt.name 
                WHERE r.discipline_name = :discipline_name AND r.kiok_approved = 1
            """
            with engine.connect() as connection:
                df = pd.read_sql_query(text(pd_query), connection, params={'discipline_name': discipline_name})

            if not df.empty:
                df['planned_volume'] = pd.to_numeric(df['people_count']) * pd.to_numeric(df['norm_per_unit'])
                df['volume'] = pd.to_numeric(df['volume'])
                overall_output_percent = (df['volume'].sum() / df['planned_volume'].sum()) * 100 if df['planned_volume'].sum() > 0 else 0
                
                overall_output_line = f"\n💡 *Общая средняя выработка: {overall_output_percent:.1f}%*"
                analysis_header = "\n🛠️ *Анализ по видам работ (факт/план | % выработки):*"
                work_summary = df.groupby('work_type_name').agg(total_volume=('volume', 'sum'), total_planned=('planned_volume', 'sum')).reset_index()
                work_summary['avg_output'] = (work_summary['total_volume'] / work_summary['total_planned'].replace(0, 1)) * 100
                
                analysis_lines = [
                    f"  - *{get_data_translation(row['work_type_name'], lang)}*:\n    `{row['total_volume']:.1f} / {row['total_planned']:.1f} | {row['avg_output']:.1f}%`"
                    for _, row in work_summary.sort_values(by='avg_output', ascending=False).iterrows()
                ]
            else:
                analysis_lines = ["\n*Данные по видам работ отсутствуют.*"]
        
        message = [
            header, "---",
            f"👤 *Пользователи в дисциплине:*\n  - Бригадиры: *{user_counts['brigades']}*\n  - ПТО: *{user_counts['pto']}*\n  - КИОК: *{user_counts['kiok']}*",
            f"\n📈 *Общая статистика по дисциплине:*\n  - Всего подано: *{total_reports}*\n  - ✅ Согласовано: *{report_stats.get('1', 0)}*\n  - ❌ Отклонено: *{report_stats.get('-1', 0)}*\n  - ⏳ Ожидает: *{report_stats.get('0', 0)}*",
            f"\n🚫 *Не сдали отчет сегодня: {non_reporters_count} бригад*",
            get_text('low_performance_brigade_count', lang).format(count=low_performance_count)
        ]

        if overall_output_line: message.append(overall_output_line)
        if analysis_header: message.append(analysis_header)
        message.extend(analysis_lines)
        final_text = "\n".join(message)

    except Exception as e:
        logger.error(f"Ошибка при генерации дашборда дисциплины: {e}")
        final_text = f"❌ {get_text('error_generic', lang)}"
    
    back_button_callback = "report_historical" if (user_role.get('isAdmin') or user_role.get('managerLevel') == 1) else "report_menu_all"
    keyboard = [[InlineKeyboardButton(get_text('back_button', lang), callback_data=back_button_callback)]]
    
    await query.edit_message_text(text=final_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def show_problem_brigades_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, selected_date: date) -> None:
    """
    Отображает меню 'Проблемные бригады' для УКАЗАННОЙ ДАТЫ.
    """
    query = update.callback_query
    chat_id = query.message.chat_id
    message_id_to_edit = query.message.message_id

    await context.bot.edit_message_text(
        chat_id=chat_id, message_id=message_id_to_edit,
        text="⏳ Собираю сводку по проблемным бригадам..."
    )

    disciplines = db_query("SELECT id, name FROM disciplines ORDER BY name")
    keyboard = []
    date_str_for_query = selected_date.strftime('%Y-%m-%d')
    date_str_for_callback = selected_date.strftime('%Y-%m-%d')
    
    summary_lines = [f"*⚠️ Проблемные бригады на {selected_date.strftime('%d.%m.%Y')}:*", ""]

    if not disciplines:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=message_id_to_edit,
            text="В системе нет дисциплин для анализа.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="handle_problem_brigades_button")]])
        )
        return

    for disc_id, disc_name in disciplines:
        non_reporters_query = "SELECT COUNT(b.user_id) FROM brigades b WHERE b.discipline = %s AND NOT EXISTS (SELECT 1 FROM reports r WHERE r.foreman_name = b.brigade_name AND r.report_date = %s)"
        non_reporters_raw = db_query(non_reporters_query, (disc_id, date_str_for_query))
        non_reporters_count = non_reporters_raw[0][0] if non_reporters_raw else 0

        if non_reporters_count > 0:
            summary_lines.append(f"🔴 *{disc_name}:* не отчитались - *{non_reporters_count}*")
        else:
            summary_lines.append(f"🟢 *{disc_name}:* все отчитались")

       
        keyboard.append([InlineKeyboardButton(f"Детально по «{disc_name}»", callback_data=f"gen_problem_report_{disc_id}_{date_str_for_callback}_1")])

    summary_lines.append("\nВыберите дисциплину для детального просмотра:")
    keyboard.append([InlineKeyboardButton("◀️ Назад к выбору даты", callback_data="handle_problem_brigades_button")])

    await context.bot.edit_message_text(
        chat_id=chat_id, message_id=message_id_to_edit,
        text="\n".join(summary_lines),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def generate_problem_brigades_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Генерирует отчет по проблемным бригадам, принимая ID дисциплины и ДАТУ из callback.
    """
    query = update.callback_query
    await query.answer()

    try:
        parts = query.data.split('_')
        discipline_id = int(parts[3])
        date_str = parts[4]
        page = int(parts[5])
        selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except (IndexError, ValueError):
        await query.edit_message_text("❌ Ошибка: Не удалось прочитать данные из кнопки.")
        return

    discipline_name_raw = db_query("SELECT name FROM disciplines WHERE id = %s", (discipline_id,))
    if not discipline_name_raw:
        await query.edit_message_text("Ошибка: Дисциплина не найдена по ID.")
        return
    discipline_name = discipline_name_raw[0][0]
    
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)
    await query.edit_message_text(f"⏳ {get_text('loading_please_wait', lang)}")

    try:
        non_reporters_query = "SELECT b.brigade_name FROM brigades b WHERE b.discipline = %s AND NOT EXISTS (SELECT 1 FROM reports r WHERE r.foreman_name = b.brigade_name AND r.report_date = %s) ORDER BY b.brigade_name"
        non_reporters_raw = db_query(non_reporters_query, (discipline_id, date_str))
        non_reporters = [row[0] for row in non_reporters_raw] if non_reporters_raw else []

        engine = create_engine(DATABASE_URL)
        pd_query = "SELECT r.foreman_name, r.people_count, r.volume, wt.norm_per_unit, wt.name as work_type_name_alias FROM reports r JOIN work_types wt ON r.work_type_name = wt.name AND r.discipline_name = (SELECT d.name FROM disciplines d WHERE d.id = wt.discipline_id) WHERE r.discipline_name = :discipline_name AND r.report_date = :report_date"
        with engine.connect() as connection:
            df = pd.read_sql_query(text(pd_query), connection, params={'discipline_name': discipline_name, 'report_date': date_str})

        low_performers_series = pd.Series(dtype='float64')
        if not df.empty:
            performance_df = df[~df['work_type_name_alias'].str.contains('Прочие', case=False, na=False)].copy()
            if not performance_df.empty:
                performance_df['planned_volume'] = pd.to_numeric(performance_df['people_count'], errors='coerce') * pd.to_numeric(performance_df['norm_per_unit'], errors='coerce')
                mask = performance_df['planned_volume'] > 0
                performance_df.loc[mask, 'output_percentage'] = (pd.to_numeric(performance_df.loc[mask, 'volume']) / performance_df.loc[mask, 'planned_volume']) * 100
                avg_performance = performance_df.groupby('foreman_name')['output_percentage'].mean()
                low_performers_series = avg_performance[avg_performance < 100].sort_values()

        back_callback = "handle_problem_brigades_button"
        
        if not non_reporters and low_performers_series.empty:
            await query.edit_message_text(
                f"На {selected_date.strftime('%d.%m.%Y')} проблемных бригад по дисциплине «{discipline_name}» не найдено.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text('back_button', lang), callback_data=back_callback)]]),
                parse_mode="Markdown"
            )
            return
        
        message_lines = [f"*Проблемные бригады по дисциплине «{discipline_name}» за {selected_date.strftime('%d.%m.%Y')}*"]
        if non_reporters:
            message_lines.append("\n*Не сдали отчет:*")
            for name in non_reporters: message_lines.append(f"- {name}")
        if not low_performers_series.empty:
            message_lines.append("\n*Низкая выработка (<100%):*")
            message_lines.append("`")
            max_name_len = max(len(name) for name in low_performers_series.index) + 2 if low_performers_series.index.any() else 20
            for name, perc in low_performers_series.items(): message_lines.append(f"{name.ljust(max_name_len)}: {perc:.1f}%")
            message_lines.append("`")

        keyboard = [[InlineKeyboardButton("◀️ Назад к выбору даты", callback_data=back_callback)]]
        await query.edit_message_text(text="\n".join(message_lines), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Ошибка при генерации отчета 'Проблемные бригады': {e}")
        await query.edit_message_text("❌ Ошибка при генерации детального отчета.")

async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает пользователю информацию о его профиле и роли ."""
    query = update.callback_query
    await query.answer()
    
    user_id = str(query.from_user.id)
    user_role = check_user_role(user_id)
    lang = get_user_language(user_id) 
    
    phone_number_str = user_role.get('phoneNumber') or 'не указан'
   
    profile_text = (
        f"*{get_text('profile_title', lang)}*\n\n"
        f"{get_text('user_id_field', lang)} `{user_id}`\n"
        f"{get_text('phone_field', lang)} `{phone_number_str}`\n"
        f"{get_text('username_field', lang)} @{query.from_user.username or 'не указан'}\n\n"
    )


    if user_role['isAdmin']:
  
        profile_text += f"{get_text('role_field', lang)} {get_text('auth_role_manager', lang)} 👑 (Админ)\n"
    elif user_role['isManager']:
        level = user_role.get('managerLevel', 'N/A')
        discipline = user_role.get('discipline') or get_text('all_disciplines', lang)
        profile_text += (
            f"{get_text('role_field', lang)} {get_text('auth_role_manager', lang)} 💼\n"
            f"{get_text('level_field', lang)} {level}\n"
            f"{get_text('discipline_field', lang)} {discipline}\n"
        )
    elif user_role['isForeman']:
        profile_text += (
            f"{get_text('role_field', lang)} {get_text('auth_role_foreman', lang)} 👷\n"
            f"{get_text('brigade_field', lang)} {user_role.get('brigadeName', '')}\n"
            f"{get_text('discipline_field', lang)} {user_role.get('discipline', '')}\n"
        )
    elif user_role['isPto']:
        profile_text += (
            f"{get_text('role_field', lang)} {get_text('auth_role_pto', lang)} 🛠️\n"
            f"{get_text('discipline_field', lang)} {user_role.get('discipline', '')}\n"
        )
    elif user_role['isKiok']:
        profile_text += (
            f"{get_text('role_field', lang)} {get_text('auth_role_kiok', lang)} ✅\n"
            f"{get_text('discipline_field', lang)} {user_role.get('discipline', '')}\n"
        )
    else:
        profile_text = get_text('error_unauthorized', lang)
        
    
    keyboard = [[InlineKeyboardButton(get_text('main_menu_title', lang), callback_data="go_back_to_main_menu")]]
    await query.edit_message_text(text=profile_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def manage_users_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает меню управления пользователями со сводкой по количеству."""
    query = update.callback_query
    await query.answer()
    

    try:
        counts = {}
        roles = ['admins', 'managers', 'brigades', 'pto', 'kiok']
        for role in roles:
            
            result = db_query(f"SELECT COUNT(*) FROM {role}")
            counts[role] = result[0][0] if result else 0
        
        summary_text = (
            f"📊 *Сводка по ролям:*\n"
            f"  ▪️ Администраторы: *{counts['admins']}*\n"
            f"  ▪️ Руководители: *{counts['managers']}*\n"
            f"  ▪️ Бригадиры: *{counts['brigades']}*\n"
            f"  ▪️ ПТО: *{counts['pto']}*\n"
            f"  ▪️ КИОК: *{counts['kiok']}*\n\n"
            f"Выберите список для просмотра:"
        )

        keyboard = [
            [InlineKeyboardButton("👑 Администраторы", callback_data="list_users_admins_1")],
            [InlineKeyboardButton("💼 Руководители", callback_data="list_users_managers_1")],
            [InlineKeyboardButton("👷 Бригадиры", callback_data="list_users_brigades_1")],
            [InlineKeyboardButton("🛠️ ПТО", callback_data="list_users_pto_1")],
            [InlineKeyboardButton("✅ КИОК", callback_data="list_users_kiok_1")],
            [InlineKeyboardButton("◀️ Назад в управление", callback_data="manage_menu")]
        ]
        
        await query.edit_message_text(
            text=summary_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

    except Exception as e:
        logger.error(f"Ошибка в manage_users_menu: {e}")
        await query.edit_message_text("❌ Произошла ошибка при загрузке данных о пользователях. Попробуйте снова.")

async def link_topic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Привязывает ID темы к дисциплине и отправляет все неотправленные отчеты по ней (PostgreSQL-совместимая версия)."""
    user_id = str(update.effective_user.id)
    chat_id = update.effective_chat.id
    
    user_role = check_user_role(user_id)
    if not user_role.get('isAdmin'):
        await update.message.reply_text("⛔️ У вас нет прав для выполнения этой команды.")
        return

    topic_id = update.message.message_thread_id
    if not topic_id:
        await update.message.reply_text("⚠️ Эту команду нужно вызывать непосредственно в теме группы.")
        return
        
    if not context.args:
        await update.message.reply_text("⚠️ Пожалуйста, укажите название дисциплины. Например: `/link_topic МК`")
        return
        
    discipline_name_input = " ".join(context.args).strip()
    

    discipline_row = db_query("SELECT name FROM disciplines WHERE name ILIKE %s", (discipline_name_input,))
    
    if not discipline_row:
        await update.message.reply_text(f"❗ Ошибка: Дисциплина «{discipline_name_input}» не найдена в справочнике.")
        return
    

    canonical_discipline_name = discipline_row[0][0]

 
    db_query(
        """
        INSERT INTO topic_mappings (discipline_name, chat_id, topic_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (discipline_name) DO UPDATE SET
            chat_id = EXCLUDED.chat_id,
            topic_id = EXcluded.topic_id;
        """,
        (canonical_discipline_name, chat_id, topic_id)
    )
    
    await update.message.reply_text(f"✅ Тема успешно привязана к дисциплине «{canonical_discipline_name}». Ищу неотправленные отчеты...")
    

    unsent_reports = db_query(
        "SELECT * FROM reports WHERE discipline_name = %s AND group_message_id IS NULL",
        (canonical_discipline_name,)
    )
    
    sent_count = 0
    if unsent_reports:
        for report_tuple in unsent_reports:
  
            (report_id, _, corpus_name, discipline_db, work_type_name, foreman_name, 
             people_count, volume, report_date, notes, _, _, _, _) = report_tuple
            
            unit_of_measure_raw = db_query(
                "SELECT unit_of_measure FROM work_types WHERE name = %s AND discipline_id = (SELECT id FROM disciplines WHERE name = %s)", 
                (work_type_name, discipline_db)
            )
            unit_of_measure = unit_of_measure_raw[0][0] if unit_of_measure_raw and unit_of_measure_raw[0][0] else ""

            report_lines = [
                f"📄 *Отложенный отчет от бригадира: {foreman_name}*\n",
                f"▪️ *Корпус:* {corpus_name}",
                f"▪️ *Дисциплина:* {discipline_db}",
                f"▪️ *Вид работ:* {work_type_name}",
                f"▪️ *Дата:* {report_date.strftime('%d.%m.%Y')}",
                f"▪️ *Кол-во человек:* {people_count}",
                f"▪️ *Выполненный объем:* {volume} {unit_of_measure}"
            ]
            if notes:
                report_lines.append(f"▪️ *Примечание:* {notes}")
            report_lines.append(f"\n*Статус:* ⏳ Ожидает согласования КИОК")
            report_text = "\n".join(report_lines)

            keyboard = [[
                InlineKeyboardButton("✅ Согласовать", callback_data=f"kiok_approve_{report_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"kiok_reject_{report_id}")
            ]]
            
            try:
                sent_message = await context.bot.send_message(
                    chat_id=chat_id, text=report_text, message_thread_id=topic_id,
                    reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
                )
                db_query("UPDATE reports SET group_message_id = %s WHERE id = %s", (sent_message.message_id, report_id))
                sent_count += 1
            except Exception as e:
                logger.error(f"Не удалось отправить отложенный отчет {report_id}: {e}")

    await update.message.reply_text(f"✅ Поиск завершен. Отправлено ранее не отправленных отчетов: *{sent_count}*.", parse_mode="Markdown")

async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает постраничный список пользователей выбранной роли с указанием их дисциплины."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    role_to_list = parts[2]
    current_page = int(parts[3])
    discipline_filter = parts[4] if len(parts) > 4 else 'all'

    table_map = {
        "admins": {"table": "admins", "title": "Администраторы"},
        "managers": {"table": "managers", "title": "Руководители"},
        "brigades": {"table": "brigades", "title": "Бригадиры"},
        "pto": {"table": "pto", "title": "ПТО"},
        "kiok": {"table": "kiok", "title": "КИОК"}
    }
    table_info = table_map.get(role_to_list)
    if not table_info:
        await query.edit_message_text("Ошибка: неизвестная роль.")
        return

    table_name = table_info['table']
    offset = (current_page - 1) * USERS_PER_PAGE


    where_clauses = []
    params = []
    if discipline_filter != 'all' and role_to_list != 'admins':
        where_clauses.append("u.discipline = %s")
        params.append(int(discipline_filter))

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)
    

    if role_to_list == 'admins':
        query_sql = f"SELECT user_id, first_name, last_name, phone_number, NULL as discipline_name FROM {table_name} ORDER BY first_name, last_name LIMIT %s OFFSET %s"
        final_params_data = [USERS_PER_PAGE, offset]
    else:
        query_sql = f"""
            SELECT u.user_id, u.first_name, u.last_name, u.phone_number, d.name as discipline_name
            FROM {table_name} u
            LEFT JOIN disciplines d ON u.discipline = d.id
            {where_sql}
            ORDER BY u.first_name, u.last_name
            LIMIT %s OFFSET %s
        """
        final_params_data = params + [USERS_PER_PAGE, offset]
    
    users = db_query(query_sql, tuple(final_params_data))


    count_table_alias = "" if role_to_list == 'admins' else "u"
    count_query = f"SELECT COUNT(*) FROM {table_name} {count_table_alias} {where_sql}"
    total_users_raw = db_query(count_query, tuple(params))
    total_users = total_users_raw[0][0] if total_users_raw else 0
    total_pages = math.ceil(total_users / USERS_PER_PAGE) if total_users > 0 else 1

    message = f"📜 *Список: {table_info['title']}* (Страница {current_page} из {total_pages})\n\n"
    
    if not users:
        message += "_Список пуст._"
    else:
        message_lines = []
        start_index = (current_page - 1) * USERS_PER_PAGE + 1
        for i, user_data in enumerate(users, start=start_index):
            _user_id, first_name, last_name, phone, discipline_name = user_data
            user_line = f"*{i}.* {first_name or ''} {last_name or ''}"
            if discipline_name:
                user_line += f" — *{discipline_name}*"
            message_lines.append(user_line)
            message_lines.append(f"    `{phone or 'телефон не указан'}`")
        message += "\n".join(message_lines)

    keyboard = []
    if users:
        message += "\n\n*Для действия выберите номер пользователя:*"
        action_buttons = []
        row = []
        for i, user_data in enumerate(users, start=1):
            user_id = user_data[0]
            row.append(InlineKeyboardButton(str(i), callback_data=f"edit_user_{role_to_list}_{user_id}"))
            if len(row) == 5: action_buttons.append(row); row = []
        if row: action_buttons.append(row)
        keyboard.extend(action_buttons)

    navigation_buttons = []
    if current_page > 1:
        navigation_buttons.append(InlineKeyboardButton("◀️", callback_data=f"list_users_{role_to_list}_{current_page - 1}_{discipline_filter}"))
    if current_page < total_pages:
        navigation_buttons.append(InlineKeyboardButton("▶️", callback_data=f"list_users_{role_to_list}_{current_page + 1}_{discipline_filter}"))
    if navigation_buttons: keyboard.append(navigation_buttons)
    

    if role_to_list in ['managers', 'brigades', 'pto', 'kiok']:
        disciplines = db_query("SELECT id, name FROM disciplines ORDER BY name")
        if disciplines:
            keyboard.append([InlineKeyboardButton("🗂️ Фильтр по дисциплинам 🗂️", callback_data="noop")])
            
 
            disc_buttons_flat = [InlineKeyboardButton(name, callback_data=f"list_users_{role_to_list}_1_{disc_id}") for disc_id, name in disciplines]
            keyboard.extend([disc_buttons_flat[i:i + 2] for i in range(0, len(disc_buttons_flat), 2)])
            
            keyboard.append([InlineKeyboardButton("Показать всех", callback_data=f"list_users_{role_to_list}_1_all")])

    keyboard.append([InlineKeyboardButton("◀️ В меню админа", callback_data="manage_users")])

    await query.edit_message_text(text=message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def set_discipline(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обновляет дисциплину, сбрасывает состояние/табель и показывает админу простое подтверждение."""
    query = update.callback_query
    await query.answer("Обновляю дисциплину...")

    parts = query.data.split('_')
    role, user_id_to_edit, new_discipline_id = parts[2], parts[3], int(parts[4])

    db_query(f"UPDATE {role} SET discipline = %s WHERE user_id = %s", (new_discipline_id, user_id_to_edit))


    try:
    
        if int(user_id_to_edit) in context._application.user_data:
            context._application.user_data[int(user_id_to_edit)].clear()
            logger.info(f"Состояние для пользователя {user_id_to_edit} было полностью сброшено из-за смены дисциплины.")
            
        if role == 'brigades':
            today_str = date.today().strftime('%Y-%m-%d')
            db_query("DELETE FROM daily_rosters WHERE brigade_user_id = %s AND roster_date = %s", (user_id_to_edit, today_str))
            logger.info(f"Табель для {user_id_to_edit} на {today_str} был сброшен из-за смены дисциплины.")
        
        discipline_name_raw = db_query("SELECT name FROM disciplines WHERE id = %s", (new_discipline_id,))
        new_discipline_name = discipline_name_raw[0][0] if discipline_name_raw else "Неизвестно"
        greeting_text = f"⚙️ Администратор изменил вашу дисциплину на «{new_discipline_name}». Пожалуйста, подайте табель заново, если уже делали это сегодня."
        

        await show_main_menu_logic(context, user_id_to_edit, int(user_id_to_edit), greeting=greeting_text)
        
    except Exception as e:
         logger.error(f"Не удалось уведомить пользователя {user_id_to_edit} о смене дисциплины. Ошибка: {e}")

 
    keyboard = [[InlineKeyboardButton(f"◀️ Назад к списку", callback_data=f"list_users_{role}_1")]]
    await query.edit_message_text(
        text=f"✅ Дисциплина изменена для `{user_id_to_edit}`.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def show_problem_brigades_by_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатывает выбор даты ('Сегодня'/'Вчера') и вызывает нужную функцию в зависимости от роли.
    """
    query = update.callback_query
    await query.answer()

    period = query.data.split('_')[-1]
    selected_date = date.today() if period == 'today' else date.today() - timedelta(days=1)

    user_role = check_user_role(str(query.from_user.id))

    if user_role.get('isAdmin') or user_role.get('managerLevel') == 1:
        await show_problem_brigades_menu(update, context, selected_date=selected_date)
   
    else:
        discipline_name = user_role.get('discipline')
        if not discipline_name:
            await query.edit_message_text("Ошибка: для вашей роли не задана дисциплина.")
            return

        discipline_id_raw = db_query("SELECT id FROM disciplines WHERE name = %s", (discipline_name,))
        if not discipline_id_raw:
            await query.edit_message_text(f"Ошибка: Не удалось найти ID для дисциплины «{discipline_name}».")
            return
        discipline_id = discipline_id_raw[0][0]

        await generate_problem_brigades_report(update, context, discipline_id=discipline_id, selected_date=selected_date, page=1)

# --- ЛОГИКА УПравления отчетами ---

async def admin_report_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начало диалога: Админ выбирает дисциплину."""
    query = update.callback_query
    await query.answer()

    disciplines = db_query("SELECT name FROM disciplines ORDER BY name")
    if not disciplines:
        await query.edit_message_text("В системе нет дисциплин для управления отчетами.")
        return ConversationHandler.END

    keyboard = [[InlineKeyboardButton(name, callback_data=f"admin_disc_{name}")] for name, in disciplines]
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_admin_op")])

    await query.edit_message_text(
        "🗂️ *Управление отчетами*\n\n*Шаг 1:* Выберите дисциплину",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return SELECT_DISC_FOR_EDIT

async def admin_select_discipline(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
       Админ выбрал дисциплину, показываем бригады постранично.
    """
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    discipline_name = parts[2]
    page = int(parts[3]) if len(parts) > 3 else 1
    
    context.user_data['admin_edit_discipline'] = discipline_name

    brigades_raw = db_query(
        """
        SELECT b.brigade_name, b.user_id, COUNT(r.id) as report_count
        FROM brigades b JOIN reports r ON b.brigade_name = r.foreman_name
        WHERE r.discipline_name = %s GROUP BY b.brigade_name, b.user_id ORDER BY b.brigade_name
        """, (discipline_name,)
    )
    
    if not brigades_raw:
        await query.edit_message_text(
            f"В дисциплине *{discipline_name}* не найдено бригад с отчетами.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="admin_report_menu_start")]])
        )
        return ConversationHandler.END 

    items_per_page = 10 
    total_items = len(brigades_raw)
    total_pages = (total_items + items_per_page - 1) // items_per_page
    start_index = (page - 1) * items_per_page
    end_index = start_index + items_per_page
    brigades_on_page = brigades_raw[start_index:end_index]

    keyboard = []
    for name, user_id, count in brigades_on_page:
        keyboard.append([InlineKeyboardButton(f"{name} (отчетов: {count})", callback_data=f"admin_brig_{user_id}")])

    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"admin_disc_{discipline_name}_{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"admin_disc_{discipline_name}_{page+1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("◀️ Назад к выбору дисциплин", callback_data="admin_report_menu_start")])
    
    await query.edit_message_text(
        f"🗂️ *Управление отчетами: {discipline_name}* (Стр. {page}/{total_pages})\n\n*Шаг 2:* Выберите бригаду",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return SELECT_BRIGADE_FOR_EDIT

async def admin_select_brigade(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Админ выбрал бригаду, показываем ее отчеты за сегодня."""
    query = update.callback_query
    await query.answer()
    brigade_user_id = query.data.split('_', 2)[-1]
    context.user_data['admin_edit_brigade_id'] = brigade_user_id

    brigade_info = db_query("SELECT brigade_name FROM brigades WHERE user_id = %s", (brigade_user_id,))
    if not brigade_info:
        await query.edit_message_text("Ошибка: Бригада не найдена.")
        return SELECT_BRIGADE_FOR_EDIT 
    
    brigade_name = brigade_info[0][0]
    context.user_data['admin_edit_brigade_name'] = brigade_name

    return await admin_show_reports_for_brigade(update, context, date.today())

async def admin_prompt_for_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запрашивает у админа дату для просмотра отчетов и добавляет кнопку 'Назад'."""
    query = update.callback_query
    await query.answer()

    brigade_user_id = context.user_data.get('admin_edit_brigade_id')
    keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"admin_brig_{brigade_user_id}")]]
    
    message = await query.edit_message_text(
        "Введите дату для просмотра отчетов в формате *ДД.ММ.ГГГГ*:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    
    context.user_data['last_prompt_message_id'] = message.message_id
  
    return SELECT_REPORT_FOR_EDIT

async def admin_process_date_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает введенную админом дату, удаляет лишние сообщения и показывает отчеты."""
    try:
        selected_date = datetime.strptime(update.message.text, "%d.%m.%Y").date()

        await update.message.delete()

        prompt_id = context.user_data.pop('last_prompt_message_id', None)
        if prompt_id:
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=prompt_id)
            except Exception:
                pass 

        return await admin_show_reports_for_brigade(update, context, selected_date)
        
    except ValueError:
        await update.message.reply_text("Неверный формат даты. Попробуйте еще раз: ДД.ММ.ГГГГ")
        return SELECT_BRIGADE_FOR_EDIT 

async def admin_show_reports_for_brigade(update: Update, context: ContextTypes.DEFAULT_TYPE, report_date: date) -> int:
    """Отображает список отчетов для выбранной бригады и даты ."""
  
    brigade_name = context.user_data['admin_edit_brigade_name']
    report_date_str = report_date.strftime('%Y-%m-%d')
    
    reports = db_query(
        "SELECT id, work_type_name, kiok_approved FROM reports WHERE foreman_name = %s AND report_date = %s ORDER BY id",
        (brigade_name, report_date_str)
    )

    date_display = report_date.strftime('%d.%m.%Y')
    message_text = f"🗂️ *Отчеты бригады \"{brigade_name}\"*\n*За дату: {date_display}*\n\n"
    
    keyboard = []
    if not reports:
        message_text += "_На эту дату отчеты не найдены._"
    else:
        message_text += "*Выберите отчет для действия:*"
        status_emoji = {1: '✅', 0: '⏳', -1: '❌'}
        for r_id, work_type, status in reports:
            emoji = status_emoji.get(status, '❓')
            report_line = f"{emoji} ID: {r_id} - {work_type[:30]}"
            keyboard.append([
                InlineKeyboardButton(report_line, callback_data=f"admin_noop_{r_id}")
            ])
            keyboard.append([
                InlineKeyboardButton("✏️ Редакт.", callback_data=f"admin_edit_{r_id}"),
                InlineKeyboardButton("🗑️ Удалить", callback_data=f"admin_delete_{r_id}")
            ])

    keyboard.append([
        InlineKeyboardButton("Сегодня", callback_data="admin_show_date_today"),
        InlineKeyboardButton("Вчера", callback_data="admin_show_date_yesterday"),
        InlineKeyboardButton("🗓️ Выбрать дату", callback_data="admin_pick_date")
    ])


    keyboard.append([InlineKeyboardButton("◀️ Назад к выбору бригады", callback_data=f"admin_disc_{context.user_data['admin_edit_discipline']}_1")])

    if update.callback_query:
        await update.callback_query.edit_message_text(
            message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
    else: 
        await context.bot.send_message(
            update.effective_chat.id, message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        
    return SELECT_REPORT_FOR_EDIT

async def admin_show_reports_by_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает нажатие кнопок 'Сегодня'/'Вчера' и показывает отчеты за нужную дату."""
    query = update.callback_query
    await query.answer()

    period = query.data.split('_')[-1]
    
    if period == 'today':
        selected_date = date.today()
    elif period == 'yesterday':
        selected_date = date.today() - timedelta(days=1)
    else:
        
        selected_date = date.today()

    return await admin_show_reports_for_brigade(update, context, selected_date)

async def admin_confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запрашивает подтверждение на удаление отчета."""
    query = update.callback_query
    await query.answer()
    report_id = query.data.split('_')[-1]
    context.user_data['admin_report_id'] = report_id

    keyboard = [
        [InlineKeyboardButton("✅ Да, удалить", callback_data="admin_delete_confirm_yes")],
        [InlineKeyboardButton("❌ Нет, отмена", callback_data=f"admin_brig_{context.user_data['admin_edit_brigade_id']}")]
    ]
    await query.edit_message_text(
        f"‼️ *Вы уверены, что хотите удалить отчет с ID {report_id}?*\n\nЭто действие необратимо. Сообщение в группе КИОК будет удалено, а бригадиру придет уведомление.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return CONFIRM_DELETE

async def admin_execute_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Выполняет удаление отчета, сообщения в группе и уведомляет бригадира."""
    query = update.callback_query
    await query.answer()
    report_id = context.user_data.get('admin_report_id')

    report_info = db_query("SELECT group_message_id, discipline_name FROM reports WHERE id = %s", (report_id,))

    if report_info and report_info[0][0]:
        group_message_id = report_info[0][0]
        discipline_name = report_info[0][1]
        topic_info = db_query("SELECT chat_id, topic_id FROM topic_mappings WHERE discipline_name = %s", (discipline_name,))
        if topic_info:
            chat_id, topic_id = topic_info[0]
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=group_message_id)
                logger.info(f"Админ {query.from_user.id} удалил сообщение {group_message_id} в чате {chat_id}")
            except Exception as e:
                logger.error(f"Не удалось удалить сообщение {group_message_id} в группе: {e}")

    db_query("DELETE FROM reports WHERE id = %s", (report_id,))

    brigade_user_id = context.user_data.get('admin_edit_brigade_id')
    try:
        await context.bot.send_message(
            chat_id=brigade_user_id,
            text=f"⚠️ *Внимание!* Ваш отчет с ID `{report_id}` был удален администратором.\n\nПожалуйста, подайте его заново с корректными данными.",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить бригадира {brigade_user_id} об удалении: {e}")

    await query.edit_message_text(f"✅ Отчет ID {report_id} успешно удален.")

    return await admin_show_reports_for_brigade(update, context, date.today())

async def cancel_admin_operation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет операцию и выходит из диалога."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Действие отменено.")
    context.user_data.clear()
 
    await show_main_menu_logic(context, str(query.from_user.id), query.message.chat_id, query.message.message_id)
    return ConversationHandler.END

async def start_report_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """ Точка входа. Загружает данные в context и вызывает функцию отображения меню. """
    query = update.callback_query
    await query.answer()


    if 'edit_report_data' not in context.user_data:
        report_id = int(query.data.split('_')[-1])
        report_data_raw = db_query("SELECT * FROM reports WHERE id = %s", (report_id,))
        if not report_data_raw:
            await query.edit_message_text("❌ Ошибка: Отчет не найден.")
            return ConversationHandler.END
        
        columns = [desc[0] for desc in db_query("SELECT column_name FROM information_schema.columns WHERE table_name = 'reports' ORDER BY ordinal_position")]
        report_data = dict(zip(columns, report_data_raw[0]))
        context.user_data['edit_report_data'] = report_data
        context.user_data['changed_fields'] = set()

    await display_edit_menu(update, context)
    return SELECT_FIELD_TO_EDIT

async def prompt_for_new_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запрашивает новое значение."""
    query = update.callback_query
    await query.answer()

    field_to_edit = query.data.split('edit_field_')[-1]
    context.user_data['field_to_edit'] = field_to_edit
    
    if field_to_edit == 'discipline_name':
        disciplines = db_query("SELECT name FROM disciplines ORDER BY name")
        keyboard = [[InlineKeyboardButton(name, callback_data=f"set_new_value_{name}")] for name, in disciplines]
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data=f"admin_edit_{context.user_data['edit_report_data']['id']}")])
        await query.edit_message_text("Выберите новую дисциплину:", reply_markup=InlineKeyboardMarkup(keyboard))
        return AWAITING_NEW_VALUE

    field_names = {
        'corpus_name': 'корпус', 'work_type_name': 'вид работ',
        'report_date': 'дату (ДД.ММ.ГГГГ)', 'people_count': 'количество человек',
        'volume': 'объем', 'notes': 'примечание'
    }
    prompt_text = f"Введите новое значение для поля *'{field_names.get(field_to_edit, field_to_edit)}'*:"
    message = await query.edit_message_text(prompt_text, parse_mode="Markdown")
    context.user_data['last_bot_message_id'] = message.message_id
    return AWAITING_NEW_VALUE

async def process_new_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает новое значение, обновляет данные отчета и возвращает к меню редактирования."""
    field = context.user_data.get('field_to_edit')
    report_data = context.user_data.get('edit_report_data')

    if field != 'people_count' and field != 'volume' and field != 'report_date' and field != 'discipline_name' and field != 'corpus_name' and field != 'work_type_name' and field != 'notes':
        logger.error(f"[EDIT FATAL] Неизвестное поле для редактирования: {field}")
        return SELECT_FIELD_TO_EDIT

    if update.callback_query:
        query = update.callback_query
        await query.answer()
        new_value = query.data.split('set_new_value_')[-1]
        report_data[field] = new_value
    else:
        new_value = update.message.text
        try:
            if field == 'people_count':
                requested_count = int(new_value)
                if requested_count <= 0: raise ValueError("Count must be positive.")
             
                user_id = context.user_data['admin_edit_brigade_id']
                report_date_str = report_data['report_date'].strftime('%Y-%m-%d')
                roster_info = db_query("SELECT total_people FROM daily_rosters WHERE brigade_user_id = %s AND roster_date = %s", (user_id, report_date_str))
                total_declared = roster_info[0][0] if roster_info else 0
                assigned_info = db_query("SELECT SUM(people_count) FROM reports WHERE foreman_name = %s AND report_date = %s AND id != %s", (report_data['foreman_name'], report_date_str, report_data['id']))
                total_assigned_others = (assigned_info[0][0] or 0) if assigned_info else 0
                available_pool = total_declared - total_assigned_others
                if requested_count > available_pool:
                    await update.message.reply_text(f"❗️Ошибка: Превышен лимит. Доступно: {available_pool} чел.")
                    return AWAITING_NEW_VALUE
                report_data['people_count'] = requested_count 
            elif field == 'volume':
                report_data['volume'] = float(new_value.replace(',', '.'))
            elif field == 'report_date':
                report_data['report_date'] = datetime.strptime(new_value, "%d.%m.%Y").date() 
            else:
                report_data[field] = new_value
        except (ValueError, TypeError):
            await update.message.reply_text("❗️ Неверный формат данных.")
            return AWAITING_NEW_VALUE
        await update.message.delete()

    context.user_data['changed_fields'].add(field)
    await display_edit_menu(update, context)
    return SELECT_FIELD_TO_EDIT

async def save_edited_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет изменения в отчете и обновляет сообщение в группе."""
    query = update.callback_query
    
    admin_id = str(query.from_user.id)
    report_data = context.user_data.get('edit_report_data')
    changed_fields = context.user_data.get('changed_fields', set())
    report_id = report_data['id']

    if not changed_fields:
        await query.answer("Вы ничего не изменили", show_alert=True)
        return SELECT_FIELD_TO_EDIT

    await query.answer("⏳ Сохраняю...")

    update_query = sql.SQL("UPDATE reports SET {} WHERE id = %s").format(
        sql.SQL(', ').join(sql.SQL("{} = %s").format(sql.Identifier(key)) for key in changed_fields)
    )
    params = [report_data[key] for key in changed_fields] + [report_id]
    
    try:
        db_query(update_query, tuple(params))
        logger.info(f"Админ {admin_id} успешно обновил поля {changed_fields} для отчета {report_id}")
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА при обновлении отчета {report_id} в БД: {e}")
        await query.edit_message_text("❌ Произошла ошибка при сохранении в базу данных.")
        return SELECT_FIELD_TO_EDIT

    final_data_dict = report_data
    
    def safe_escape(text):
        if text is None:
            return ""

        return escape_markdown(str(text), version=2)

    admin_name_raw = db_query("SELECT first_name, last_name FROM admins WHERE user_id = %s", (admin_id,))
    admin_name = safe_escape(f"{admin_name_raw[0][0]} {admin_name_raw[0][1]}" if admin_name_raw else "Администратор")
    
    foreman_name_safe = safe_escape(final_data_dict['foreman_name'])
    corpus_name_safe = safe_escape(final_data_dict['corpus_name'])
    discipline_name_safe = safe_escape(final_data_dict['discipline_name'])
    work_type_safe = safe_escape(final_data_dict['work_type_name'])
    notes_safe = safe_escape(final_data_dict['notes'])
    
    unit_of_measure_raw = db_query("SELECT unit_of_measure FROM work_types WHERE name = %s", (final_data_dict['work_type_name'],))
    unit = safe_escape(unit_of_measure_raw[0][0] if unit_of_measure_raw and unit_of_measure_raw[0][0] else "")
    
    date_str_safe = safe_escape(final_data_dict['report_date'].strftime('%d.%m.%Y'))
    volume_str_safe = safe_escape(str(final_data_dict['volume']))
    people_count_safe = final_data_dict['people_count']

    report_lines = [
        f"📄 *Отчет от бригадира: {foreman_name_safe}* \\(ID: {report_id}\\)\n",
        f"▪️ *Корпус:* {corpus_name_safe}",
        f"▪️ *Дисциплина:* {discipline_name_safe}",
        f"▪️ *Вид работ:* {work_type_safe}",
        f"▪️ *Дата:* {date_str_safe}",
        f"▪️ *Кол\\-во человек:* {people_count_safe}",
        f"▪️ *Выполненный объем:* {volume_str_safe} {unit}",
    ]

    if notes_safe.strip():
        report_lines.append(f"▪️ *Примечание:* {notes_safe}")

    status_map = {1: '✅ Согласовано', 0: '⏳ Ожидает', -1: '❌ Отклонено'}
    status_text_safe = safe_escape(status_map.get(final_data_dict['kiok_approved'], 'Неизвестно'))
    
    edit_time = safe_escape(datetime.now(pytz.timezone('Asia/Tashkent')).strftime('%d.%m.%Y в %H:%M'))
    footer = f"Отредактировал: {admin_name} \\({edit_time}\\)"

    report_lines.extend(["\n", f"*Статус:* {status_text_safe}", f"_{footer}_"])
    final_text = "\n".join(report_lines)

    topic_info_raw = db_query("SELECT chat_id, topic_id FROM topic_mappings WHERE discipline_name = %s", (final_data_dict['discipline_name'],))
    if topic_info_raw and final_data_dict.get('group_message_id'):
        chat_id, topic_id = topic_info_raw[0]
        try:
            original_buttons = None
            if final_data_dict['kiok_approved'] == 0:
                original_buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Согласовать", callback_data=f"kiok_approve_{report_id}")],
                    [InlineKeyboardButton("❌ Отклонить", callback_data=f"kiok_reject_{report_id}")]
                ])

            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=final_data_dict['group_message_id'],
                text=final_text, parse_mode='MarkdownV2', reply_markup=original_buttons
            )
            logger.info(f"Сообщение для отчета {report_id} в группе {chat_id} успешно обновлено.")
        except Exception as e:
            logger.error(f"Не удалось обновить сообщение в группе для отчета {report_id}: {e}")
            await query.message.reply_text(f"⚠️ Не удалось обновить сообщение в группе КИОК. Ошибка: {e}")

    await query.answer("✅ Сохранено!", show_alert=True)

    context.user_data.pop('edit_report_data', None)
    context.user_data.pop('changed_fields', None)
    
    report_date_obj = final_data_dict['report_date']
    return await admin_show_reports_for_brigade(update, context, report_date=report_date_obj)

async def cancel_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет редактирование и возвращает к списку отчетов бригады."""
    query = update.callback_query
    await query.answer()
    
    context.user_data.pop('edit_report_data', None)
    context.user_data.pop('changed_fields', None)

    return await admin_show_reports_for_brigade(update, context, date.today())

async def display_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отображает меню редактирования. (Версия 4.0)"""
    report_data = context.user_data['edit_report_data']

    def marker(field_name):
        return "✏️" if field_name in context.user_data.get('changed_fields', set()) else "▪️"

    date_display = report_data['report_date'].strftime("%d.%m.%Y")
    text_lines = [
        f"✏️ *Редактирование отчета ID: {report_data['id']}*", "---",
        f"{marker('corpus_name')} *Корпус:* {report_data['corpus_name']}",
        f"{marker('discipline_name')} *Дисциплина:* {report_data['discipline_name']}",
        f"{marker('work_type_name')} *Вид работ:* {report_data['work_type_name']}",
        f"{marker('report_date')} *Дата:* {date_display}",
        f"{marker('people_count')} *Кол-во чел.:* {report_data['people_count']}",
        f"{marker('volume')} *Объем:* {report_data['volume']}",
        f"{marker('notes')} *Примечание:* {report_data['notes'] or 'нет'}", "---",
        "*Какое поле вы хотите изменить?*",
    ]
    keyboard = [
        [InlineKeyboardButton("Корпус", callback_data="edit_field_corpus_name"), InlineKeyboardButton("Дисциплину", callback_data="edit_field_discipline_name")],
        [InlineKeyboardButton("Вид работ", callback_data="edit_field_work_type_name"), InlineKeyboardButton("Дату", callback_data="edit_field_report_date")],
        [InlineKeyboardButton("Кол-во человек", callback_data="edit_field_people_count"), InlineKeyboardButton("Объем", callback_data="edit_field_volume")],
        [InlineKeyboardButton("Примечание", callback_data="edit_field_notes")],
        [InlineKeyboardButton("✅ Сохранить и завершить", callback_data="edit_save")],
        [InlineKeyboardButton("❌ Отмена", callback_data="edit_cancel")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(text="\n".join(text_lines), reply_markup=reply_markup, parse_mode="Markdown")
    else:
        last_bot_msg_id = context.user_data.pop('last_bot_message_id', None)
        if last_bot_msg_id:
            try: await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=last_bot_msg_id)
            except Exception: pass
        await context.bot.send_message(update.effective_chat.id, "\n".join(text_lines), reply_markup=reply_markup, parse_mode="Markdown")


# --- ЛОГИКА РЕГИСТРАЦИИ ---

async def start_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начало процесса авторизации. Спрашивает роль (МНОГОЯЗЫЧНАЯ ВЕРСИЯ)."""
    query = update.callback_query
    await query.answer()

    lang = 'ru' 
    
    keyboard = [
        [InlineKeyboardButton(get_text('auth_role_manager', lang), callback_data="auth_manager")],
        [InlineKeyboardButton(get_text('auth_role_foreman', lang), callback_data="auth_foreman")],
        [InlineKeyboardButton(get_text('auth_role_pto', lang), callback_data="auth_pto")],
        [InlineKeyboardButton(get_text('auth_role_kiok', lang), callback_data="auth_kiok")],
        [InlineKeyboardButton(get_text('cancel_button', lang), callback_data="cancel_auth")],
    ]
    await query.edit_message_text(
        text=f"*{get_text('auth_prompt_role', lang)}*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    return SELECTING_ROLE

async def select_role(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает роль и запрашивает ФИО (МНОГОЯЗЫЧНАЯ ВЕРСИЯ)."""
    query = update.callback_query
    await query.answer()

    lang = 'ru'

    role = query.data.split('_')[1]
    context.user_data['role'] = role

    prompt_text = get_text('auth_prompt_name', lang)
    
    sent_message = await query.edit_message_text(text=prompt_text, parse_mode='Markdown')
    context.user_data['last_bot_message_id'] = sent_message.message_id
    
    return GETTING_NAME

async def get_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает ФИО и запрашивает контакт."""
    user_input = update.message.text
    chat_id = update.effective_chat.id
    lang = 'ru'

    last_bot_message_id = context.user_data.pop('last_bot_message_id', None)
    if last_bot_message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=last_bot_message_id)
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение {last_bot_message_id}: {e}")

    await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)

    if ' ' not in user_input:
        
        error_text = get_text('auth_error_name', lang)
        
        sent_message = await context.bot.send_message(
            chat_id=chat_id, 
            text=error_text, 
            parse_mode="Markdown"
        )

        context.user_data['last_bot_message_id'] = sent_message.message_id
        return GETTING_NAME
        
    first_name, last_name = user_input.split(' ', 1)
    context.user_data['first_name'] = first_name
    context.user_data['last_name'] = last_name

    contact_button = KeyboardButton(text=get_text('auth_contact_button', lang), request_contact=True)
    reply_markup = ReplyKeyboardMarkup([[contact_button]], resize_keyboard=True, one_time_keyboard=True)
    
    prompt_text = get_text('auth_prompt_contact', lang)
    
    sent_message = await context.bot.send_message(
        chat_id=chat_id, 
        text=f"*{prompt_text}*", 
        reply_markup=reply_markup, 
        parse_mode="Markdown"
    )
    context.user_data['last_bot_message_id'] = sent_message.message_id
    
    return GETTING_CONTACT

async def get_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Чистая функция: получает контакт, убирает за собой все сообщения и передает управление."""
    chat_id = update.effective_chat.id
    contact = update.message.contact
    user_id_str = str(update.effective_user.id)

    await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    last_bot_message_id = context.user_data.pop('last_bot_message_id', None)
    if last_bot_message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=last_bot_message_id)
        except Exception: pass

    temp_msg = await context.bot.send_message(chat_id, "...", reply_markup=ReplyKeyboardRemove())
    await context.bot.delete_message(chat_id=chat_id, message_id=temp_msg.message_id)

    user_info = {
        "user_id": user_id_str,
        "first_name": context.user_data.get('first_name', ''),
        "last_name": context.user_data.get('last_name', ''),
        "username": update.effective_user.username or "не указан",
        "phone_number": contact.phone_number,
        "role": context.user_data.get('role')
    }
    context.bot_data[user_id_str] = user_info
    role = user_info['role']

    if role == 'manager':
        return await ask_manager_level(update, context)
    elif role in ['foreman', 'pto', 'kiok']:
        return await ask_discipline(update, context)
    
    return ConversationHandler.END
   
async def ask_manager_level(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """ЗАДАЕТ ВОПРОС про уровень руководителя."""
    chat_id = update.effective_chat.id
    lang = 'ru' 

    text = f"*{get_text('auth_prompt_manager_level', lang)}*"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(get_text('auth_manager_level1', lang), callback_data="level_1")],
        [InlineKeyboardButton(get_text('auth_manager_level2', lang), callback_data="level_2")],
    ])
    sent_message = await context.bot.send_message(chat_id, text, reply_markup=keyboard, parse_mode='Markdown')
    context.user_data['last_bot_message_id'] = sent_message.message_id
    return SELECTING_MANAGER_LEVEL

async def handle_manager_level(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает выбор уровня. Редактирует сообщение, чтобы убрать кнопки.
    """
    query = update.callback_query
    await query.answer()
    lang = 'ru'
    
    user_id_str = str(query.from_user.id)
    level = int(query.data.split('_')[1])
    user_info = context.bot_data.get(user_id_str, {})
    user_info['level'] = level
    context.bot_data[user_id_str] = user_info

    if level == 2:
        return await ask_discipline(update, context, from_manager=True)


    pending_text = get_text('auth_pending_approval', lang)
    await query.edit_message_text(text=pending_text, parse_mode='Markdown')

    emoji_message = await context.bot.send_message(chat_id=user_id_str, text="⏳")
    user_info['pending_message_ids'] = [emoji_message.message_id] 
    context.bot_data[user_id_str] = user_info

    request_text = (
        f"🔐 *Запрос на регистрацию*\n\n"
        f"▪️ *Роль:* Руководитель (Уровень 1)\n"
        f"▪️ *Имя:* {user_info.get('first_name')} {user_info.get('last_name')}\n"
        f"▪️ *Username:* @{user_info.get('username', 'не указан')}\n"
        f"▪️ *Телефон:* {user_info.get('phone_number')}\n"
        f"▪️ *UserID:* `{user_id_str}`"
    )

    await send_approval_request(
        context,
        user_id_str,
        request_text,
        f"approve_manager_{user_id_str}",
        f"reject_manager_{user_id_str}"
    )
    
    return ConversationHandler.END

async def ask_discipline(update: Update, context: ContextTypes.DEFAULT_TYPE, from_manager: bool = False) -> int:
    """ЗАДАЕТ ВОПРОС про дисциплину, используя кнопки с ID ."""
    chat_id = update.effective_chat.id if update.effective_chat else update.callback_query.message.chat_id
    lang = 'ru' 
    
    user_info = context.bot_data.get(str(chat_id), {})
    role = user_info.get('role')

    role_rus_map = {
        'foreman': get_text('auth_role_foreman', lang), 
        'pto': get_text('auth_role_pto', lang), 
        'kiok': get_text('auth_role_kiok', lang)
    }
    role_rus = role_rus_map.get(role, role.upper() if role else 'НЕИЗВЕСТНО')
    
    if from_manager:
        text = f"*{get_text('auth_prompt_discipline_manager', lang)}*"
    else:
        text = f"*{get_text('auth_prompt_discipline', lang).format(role=role_rus)}*"
        
    disciplines_from_db = db_query("SELECT id, name FROM disciplines ORDER BY name")
    
    if not disciplines_from_db:

        await context.bot.send_message(chat_id, "⚠️ *Ошибка:* В базе данных не найдено ни одной дисциплины. Обратитесь к администратору.")
        return ConversationHandler.END

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(d_name, callback_data=f"disc_{disc_id}")] for disc_id, d_name in disciplines_from_db
    ])
    
    if update.callback_query and update.callback_query.message:
         sent_message = await update.callback_query.edit_message_text(text=text, reply_markup=keyboard, parse_mode='Markdown')
    else:
         sent_message = await context.bot.send_message(chat_id, text, reply_markup=keyboard, parse_mode='Markdown')
         
    context.user_data['last_bot_message_id'] = sent_message.message_id
    return SELECTING_DISCIPLINE

async def handle_discipline(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """ОБРАБАТЫВАЕТ ВЫБОР дисциплины и отправляет запрос админу ."""
    query = update.callback_query
    await query.answer()
    await query.delete_message()
    
    lang = 'ru' 
    user_id_str = str(query.from_user.id)
    discipline_id = int(query.data.split('_')[1])
    
    user_info = context.bot_data.get(user_id_str, {})
    user_info['discipline'] = discipline_id
    role = user_info.get('role')
    context.bot_data[user_id_str] = user_info

    pending_text = get_text('auth_pending_approval', lang)
    text_message = await context.bot.send_message(chat_id=user_id_str, text=pending_text, parse_mode='Markdown')
    emoji_message = await context.bot.send_message(chat_id=user_id_str, text="⏳")
    user_info['pending_message_ids'] = [text_message.message_id, emoji_message.message_id]
    context.bot_data[user_id_str] = user_info
    
    discipline_name_raw = db_query("SELECT name FROM disciplines WHERE id = %s", (discipline_id,))
    discipline_name_for_text = discipline_name_raw[0][0] if discipline_name_raw else "ID: " + str(discipline_id)

    role_rus_map = {'manager': 'Руководителя (Ур. 2)', 'foreman': 'Бригадира', 'pto': 'ПТО', 'kiok': 'КИОК'}
    role_rus = role_rus_map.get(role, 'Неизвестно')

    request_text = (
        f"🔐 *Запрос на регистрацию*\n\n"
        f"▪️ *Роль:* {role_rus}\n"
        f"▪️ *Дисциплина:* {discipline_name_for_text}\n"
        f"▪️ *Имя:* {user_info.get('first_name')} {user_info.get('last_name')}\n"
        f"▪️ *Username:* @{user_info.get('username', 'не указан')}\n"
        f"▪️ *Телефон:* {user_info.get('phone_number')}\n"
        f"▪️ *UserID:* `{user_id_str}`"
    )

    await send_approval_request(
        context,
        user_id_str,
        request_text,
        f"approve_{role}_{user_id_str}",
        f"reject_{role}_{user_id_str}"
    )

    return ConversationHandler.END

async def cancel_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет процесс регистрации и СРАЗУ возвращает в главное меню."""
    query = update.callback_query

    await query.answer("❌ Регистрация отменена")

    await show_main_menu_logic(
        context=context,
        user_id=str(query.from_user.id),
        chat_id=query.message.chat_id,
        message_id_to_edit=query.message.message_id
    )

    context.user_data.clear()
    return ConversationHandler.END

async def start_roster_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает диалог подачи табеля ."""
    query = update.callback_query
    await query.answer()

    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)
    user_role = check_user_role(user_id)
    discipline_name = user_role.get('discipline')

    discipline_id_raw = db_query("SELECT id FROM disciplines WHERE name = %s", (discipline_name,))
    if not discipline_id_raw:
        await query.edit_message_text("⚠️ Ошибка: не удалось определить вашу дисциплину.")
        return ConversationHandler.END
    discipline_id = discipline_id_raw[0][0]

    today_str = date.today().strftime('%Y-%m-%d')
    existing_roster = db_query("SELECT id FROM daily_rosters WHERE brigade_user_id = %s AND roster_date = %s", (user_id, today_str))
    if existing_roster:
        await query.edit_message_text(
            get_text('roster_already_submitted', lang),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text('back_to_main_menu_button', lang), callback_data="go_back_to_main_menu")]])
        )
        return ConversationHandler.END

    roles_raw = db_query("SELECT role_name FROM personnel_roles WHERE discipline_id = %s ORDER BY role_name", (discipline_id,))
    if not roles_raw:
        await query.edit_message_text("⚠️ Для вашей дисциплины не настроены должности. Обратитесь к администратору.")
        return ConversationHandler.END

    role_names_ordered = [role[0] for role in roles_raw]
    context.user_data['ordered_roles_for_roster'] = role_names_ordered
    roles_text_list = [f"  *{i+1}. {name}*" for i, name in enumerate(role_names_ordered)]
    
    message_text = get_text('roster_prompt', lang).format(
        date=date.today().strftime('%d.%m.%Y'),
        roles_list="\n".join(roles_text_list)
    )

    await query.edit_message_text(text=message_text, parse_mode="Markdown")
    context.user_data['last_bot_message_id'] = query.message.message_id
    
    return AWAITING_ROLES_COUNT

async def get_role_counts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает введенные числа через запятую ."""
    user_input = update.message.text
    chat_id = update.effective_chat.id
    user_id = str(update.effective_user.id)
    lang = get_user_language(user_id)

    await update.message.delete()
    last_bot_message_id = context.user_data.pop('last_bot_message_id', None)
    if last_bot_message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=last_bot_message_id)
        except Exception: pass
    
    ordered_roles = context.user_data.get('ordered_roles_for_roster', [])
    
    try:
        counts_str = [s.strip() for s in user_input.split(',')]
        if not all(s.isdigit() for s in counts_str):
             raise ValueError("Contains non-digits")
        counts_int = [int(s) for s in counts_str]

        if len(counts_int) != len(ordered_roles):
            await update.message.reply_text(
                get_text('roster_error_mismatch', lang).format(input_count=len(counts_int), expected_count=len(ordered_roles)),
                parse_mode="Markdown"
            )
            return AWAITING_ROLES_COUNT

        parsed_roles = {role: count for role, count in zip(ordered_roles, counts_int) if count > 0}
        total_people = sum(parsed_roles.values())
        
        if not parsed_roles or total_people == 0:
            await update.message.reply_text(get_text('roster_error_no_people', lang), parse_mode="Markdown")
            return AWAITING_ROLES_COUNT

        context.user_data['roster_summary'] = { 'details': parsed_roles, 'total': total_people }

        summary_text_details = "\n".join([f"▪️ {role}: {count} чел." for role, count in parsed_roles.items()])
        summary_text = get_text('roster_confirm_prompt', lang).format(summary=summary_text_details, total=total_people)

        keyboard = [
            [InlineKeyboardButton(get_text('roster_confirm_and_save_button', lang), callback_data="confirm_roster")],
            [InlineKeyboardButton(get_text('cancel_button', lang), callback_data="cancel_roster")]
        ]
        await update.message.reply_text(summary_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        
        return CONFIRM_ROSTER

    except (ValueError, IndexError):
        await update.message.reply_text(get_text('roster_error_invalid_format', lang), parse_mode="Markdown")
        return AWAITING_ROLES_COUNT

async def save_roster(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Проверяет данные табеля и решает, как сохранять ."""
    query = update.callback_query
    await query.answer()
    
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)
    user_role = check_user_role(user_id)
    roster_summary = context.user_data.get('roster_summary')

    if not roster_summary:
        await query.edit_message_text(get_text('data_not_found_error', lang))
        return ConversationHandler.END

    today_str = date.today().strftime('%Y-%m-%d')
    total_people_new = roster_summary['total']
    
    brigade_name_for_query = user_role.get('brigadeName') or f"Бригада пользователя {user_id}"
    assigned_info = db_query("SELECT SUM(people_count) FROM reports WHERE foreman_name = %s AND report_date = %s", (brigade_name_for_query, today_str))
    total_assigned = assigned_info[0][0] or 0 if assigned_info else 0

    if total_people_new >= total_assigned:
        reserve = total_people_new - total_assigned
        greeting_text = get_text('roster_saved_safely', lang).format(reserve=reserve)
        return await execute_final_roster_save(update, context, greeting=greeting_text)

    else:
        warning_text = get_text('roster_dangerous_save_warning', lang).format(new_total=total_people_new, assigned=total_assigned)
        keyboard = [
            [InlineKeyboardButton(get_text('roster_force_save_button', lang), callback_data="force_save_roster")],
            [InlineKeyboardButton(get_text('cancel_button', lang), callback_data="cancel_roster")]
        ]
        await query.edit_message_text(warning_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        return CONFIRM_DANGEROUS_ROSTER_SAVE

async def cancel_roster_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет диалог подачи табеля ."""
    query = update.callback_query
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)

    await query.answer(get_text('action_cancelled', lang))
    await show_main_menu_logic(context, user_id, query.message.chat_id, query.message.message_id)
    context.user_data.clear()
    return ConversationHandler.END

# --- ОБРАБОТКА ПОДТВЕРЖДЕНИЯ/ОТКЛОНЕНИЯ ---

async def handle_approval(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает подтверждение/отклонение и отправляет уведомление пользователю на его языке."""
    query = update.callback_query
    
    approver_id = str(query.from_user.id)
    approver_role = check_user_role(approver_id)
    
    if not approver_role.get('isAdmin'):
        await query.answer("⛔️ У вас нет прав для выполнения этого действия.", show_alert=True)
        return

    await query.answer()
    
    parts = query.data.split('_')
    action, role, user_id = parts[0], parts[1], parts[2]
    
    user_info_to_approve = context.bot_data.get(user_id)
    if not user_info_to_approve:
        await query.edit_message_text(f"⚠️ *Не удалось найти данные для пользователя {user_id}. Запрос мог устареть.*")
        return

    pending_ids = user_info_to_approve.get('pending_message_ids', [])
    if pending_ids:
        for message_id in pending_ids:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=message_id)
            except Exception as e:
                logger.info(f"Не удалось удалить сообщение ожидания: {e}")

    admin_lang = get_user_language(approver_id)

    role_rus_map = {'manager': 'Руководитель', 'foreman': 'Бригадир', 'pto': 'ПТО', 'kiok': 'КИОК'}
    role_text = role_rus_map.get(role, role)

    if action == 'approve':
        level = user_info_to_approve.get('level')
        discipline = user_info_to_approve.get('discipline')

        update_user_role(user_id, role, user_info_to_approve, discipline, level)

        admin_confirmation_text = get_text('auth_request_approved_admin', admin_lang).format(role=role_text, name=user_info_to_approve.get('first_name'))
        await query.edit_message_text(f"*{admin_confirmation_text}*")

        user_lang = get_user_language(user_id)
        greeting_text = get_text('auth_role_approved_user', user_lang).format(role=get_text(f'auth_role_{role}', user_lang))

        await show_main_menu_logic(context, user_id=user_id, chat_id=int(user_id), greeting=greeting_text)

    elif action == 'reject':

        admin_confirmation_text = get_text('auth_request_rejected_admin', admin_lang).format(name=user_info_to_approve.get('first_name'))
        await query.edit_message_text(f"*{admin_confirmation_text}*")
        
        lang = 'ru'
        rejection_text = get_text('auth_request_rejected_user', lang).format(role=get_text(f'auth_role_{role}', lang))
        keyboard = [[InlineKeyboardButton(get_text('main_menu_title', lang), callback_data="go_back_to_main_menu")]]
        
        await context.bot.send_message(user_id, f"*{rejection_text}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
             
    if user_id in context.bot_data:
        del context.bot_data[user_id]
        logger.info(f"[APPROVE] Роль: {role}, Данные: {user_info_to_approve}")

async def list_reports_for_deletion(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает постраничный список отчетов для возможного удаления."""
    query = update.callback_query
    await query.answer()

    page = int(query.data.split('_')[-1])
    user_id = str(query.from_user.id)
    user_role = check_user_role(user_id)
    items_per_page = 5
    offset = (page - 1) * items_per_page

    base_query = "FROM reports "
    where_clauses = []
    params = []

    if user_role.get('managerLevel') == 2:
        where_clauses.append("discipline_name = %s")
        params.append(user_role.get('discipline'))
    elif user_role.get('isPto'):
        where_clauses.append("discipline_name = %s")
        params.append(user_role.get('discipline'))
        
    if where_clauses:
        base_query += "WHERE " + " AND ".join(where_clauses)

    count_query = "SELECT COUNT(*) " + base_query
    total_items_raw = db_query(count_query, tuple(params))
    total_items = total_items_raw[0][0] if total_items_raw else 0
    total_pages = math.ceil(total_items / items_per_page) if total_items > 0 else 1

    data_query = "SELECT id, report_date, foreman_name, work_type_name " + base_query + "ORDER BY id DESC LIMIT %s OFFSET %s"
    final_params = params + [items_per_page, offset]
    reports = db_query(data_query, tuple(final_params))

    message_text = f"🗑️ *Выберите отчет для удаления* (Стр. {page}/{total_pages})\n"
    keyboard = []
    if not reports:
        message_text += "\n_Отчетов для удаления не найдено._"
    else:
        for report_id, report_date, foreman, work_type in reports:
            date_str = report_date.strftime('%d.%m')
            button_text = f"ID:{report_id} ({date_str}) - {foreman} - {work_type[:20]}..."
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"confirm_delete_{report_id}")])

    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"delete_report_list_{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"delete_report_list_{page+1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="report_menu_all")])
    await query.edit_message_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def confirm_delete_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запрашивает подтверждение на удаление отчета."""
    query = update.callback_query
    await query.answer()
    report_id = query.data.split('_')[-1]

    report_info = db_query("SELECT report_date, foreman_name, work_type_name FROM reports WHERE id = %s", (report_id,))
    if not report_info:
        await query.edit_message_text("❌ Ошибка: этот отчет уже удален.")
        return

    date, foreman, work = report_info[0]
    text = (
        f"‼️ *Вы уверены, что хотите удалить этот отчет?*\n\n"
        f"▪️ ID: {report_id}\n"
        f"▪️ Дата: {date.strftime('%d.%m.%Y')}\n"
        f"▪️ Бригадир: {foreman}\n"
        f"▪️ Вид работ: {work}\n\n"
        f"Это действие необратимо."
    )
    keyboard = [
        [InlineKeyboardButton("✅ Да, удалить", callback_data=f"execute_delete_{report_id}")],
        [InlineKeyboardButton("❌ Нет, вернуться к списку", callback_data="delete_report_list_1")]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def execute_delete_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Выполняет удаление отчета и показывает простое подтверждение."""
    query = update.callback_query
    await query.answer("Удаляю...")
    report_id = query.data.split('_')[-1]

    db_query("DELETE FROM reports WHERE id = %s", (report_id,))
    logger.info(f"Пользователь {query.from_user.id} удалил отчет с ID {report_id}")

    keyboard = [[InlineKeyboardButton("◀️ Назад к списку отчетов", callback_data="delete_report_list_1")]]
    await query.edit_message_text(
        "✅ Отчет успешно удален.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def confirm_reset_roster(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запрашивает подтверждение на сброс табеля, ПРОВЕРЯЯ ПРАВА."""
    query = update.callback_query

    admin_user_id = str(query.from_user.id)
    admin_role = check_user_role(admin_user_id)

    if not (admin_role.get('isAdmin') or admin_role.get('managerLevel') == 2 or admin_role.get('isPto')):
        await query.answer("⛔️ У вас нет прав для выполнения этого действия.", show_alert=True)
        return

    await query.answer()
    user_id_to_reset = query.data.split('_')[-1]

    user_data = db_query("SELECT first_name, last_name FROM brigades WHERE user_id = %s", (user_id_to_reset,))
    if not user_data:
        await query.edit_message_text("❌ Ошибка: бригадир не найден.")
        return
        
    full_name = f"{user_data[0][0]} {user_data[0][1]}"
    
    text = (
        f"‼️ *Вы уверены, что хотите сбросить сегодняшний табель для бригадира {full_name}?*\n\n"
        f"Он сможет подать его заново. Это действие необратимо."
    )
    keyboard = [
        [InlineKeyboardButton("✅ Да, сбросить", callback_data=f"execute_reset_roster_{user_id_to_reset}")],
        [InlineKeyboardButton("❌ Нет, вернуться назад", callback_data=f"edit_user_brigades_{user_id_to_reset}")]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def execute_reset_roster(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Удаляет сегодняшний табель для бригадира и добавляет кнопку Назад."""
    query = update.callback_query
    
    admin_user_id = str(query.from_user.id)
    admin_role = check_user_role(admin_user_id)
    
    if not (admin_role.get('isAdmin') or admin_role.get('managerLevel') == 2 or admin_role.get('isPto')):
        await query.answer("⛔️ У вас нет прав для выполнения этого действия.", show_alert=True)
        return

    await query.answer("Сбрасываю табель...")
    user_id_to_reset = query.data.split('_')[-1]
    
    today_str = date.today().strftime('%Y-%m-%d')
    
    db_query("DELETE FROM daily_rosters WHERE brigade_user_id = %s AND roster_date = %s", (user_id_to_reset, today_str))
    
    logger.info(f"Админ {query.from_user.id} сбросил табель для пользователя {user_id_to_reset}")
 
    keyboard = [[InlineKeyboardButton("◀️ Назад к пользователю", callback_data=f"edit_user_brigades_{user_id_to_reset}")]]
    await query.edit_message_text(
        "✅ Табель на сегодня успешно сброшен.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    greeting_text = "⚠️ Администратор сбросил ваш сегодняшний табель. Пожалуйста, подайте его заново."
    await force_user_to_main_menu(context, user_id_to_reset, greeting_text)

# --- EXCEL---
async def export_reports_to_excel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Формирует и отправляет Excel-файл с отчетами из PostgreSQL."""
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    user_id = str(query.from_user.id)
    user_role = check_user_role(user_id)

    if not (user_role.get('isPto') or user_role.get('isKiok') or user_role.get('isAdmin')):
        await query.answer("⛔️ У вас нет прав для выполнения этого действия.", show_alert=True)
        return
        
    wait_msg = await query.edit_message_text("⏳ Формирую Excel-файл...")
    
    formatted_file_path = None
    try:
        engine = create_engine(DATABASE_URL)
        
        query_text = "SELECT * FROM reports"
        params = {}
        if (user_role.get('isPto') or user_role.get('isKiok')) and not user_role.get('isAdmin'):
            discipline = user_role.get('discipline')
            query_text += " WHERE discipline_name = :discipline"
            params = {'discipline': discipline}

        with engine.connect() as connection:
            df = pd.read_sql_query(text(query_text), connection, params=params)

        if df.empty:
            await wait_msg.edit_text("ℹ️ Не найдено ни одного отчета по вашему запросу.")
            return

        current_date_str = date.today().strftime('%Y-%m-%d')
        formatted_file_path = os.path.join(TEMP_DIR, f"formatted_report_{user_id}_{current_date_str}.xlsx")
        formatted_df = format_dataframe_for_excel(df.copy(), 'reports')

        with pd.ExcelWriter(formatted_file_path, engine='xlsxwriter') as writer:
            formatted_df.to_excel(writer, sheet_name='Отчеты по работам', index=False)
            worksheet = writer.sheets['Отчеты по работам']
            
            for i, col in enumerate(formatted_df.columns):

                if not formatted_df[col].empty:
                    max_len = formatted_df[col].astype(str).map(len).max()
                else:
                    max_len = 0
                column_len = max(max_len, len(col)) + 2
                worksheet.set_column(i, i, column_len)

        await context.bot.send_document(
            chat_id=chat_id, document=open(formatted_file_path, 'rb'), 
            filename=f"Отчет_по_работам_{current_date_str}.xlsx"
        )
        await wait_msg.delete()

    except Exception as e:
        logger.error(f"Ошибка при экспорте отчетов: {e}")
        await wait_msg.edit_text("❌ Произошла ошибка при формировании файла.")
    finally:
        if formatted_file_path and os.path.exists(formatted_file_path):
            os.remove(formatted_file_path)

async def export_full_db_to_excel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    user_id = str(query.from_user.id)
        
    if user_id != OWNER_ID:
        await query.answer("⛔️ Эта команда доступна только создателю бота.", show_alert=True)
        return
        
    wait_msg = await query.edit_message_text("⏳ Начинаю полный экспорт. Это может занять до минуты...")

    raw_file_path = None
    formatted_file_path = None
    try:
        table_names = ALL_TABLE_NAMES_FOR_BACKUP
        current_date_str = date.today().strftime('%Y-%m-%d')
        
        engine = create_engine(DATABASE_URL)

        raw_file_path = os.path.join(TEMP_DIR, f"raw_full_db_{user_id}_{current_date_str}.xlsx")
        with pd.ExcelWriter(raw_file_path, engine='xlsxwriter') as writer:
            with engine.connect() as connection:
                for table_name in table_names:
                    query_check_table = text("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = :table_name)")
                    if connection.execute(query_check_table, {'table_name': table_name}).scalar():
                        df = pd.read_sql_query(text(f"SELECT * FROM {table_name}"), connection)

                        if table_name == 'reports':
                            timezone_cols = ['timestamp', 'kiok_approval_timestamp']
                            for col in timezone_cols:
                                if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
                                    if df[col].dt.tz is not None:
                                        df[col] = df[col].dt.tz_localize(None)

                        df.to_excel(writer, sheet_name=table_name, index=False)
        
        await context.bot.send_document(chat_id=user_id, document=open(raw_file_path, 'rb'), filename=f"Полная_выгрузка_БД_raw_{current_date_str}.xlsx")

        formatted_file_path = os.path.join(TEMP_DIR, f"formatted_full_db_{user_id}_{current_date_str}.xlsx")
        with pd.ExcelWriter(formatted_file_path, engine='xlsxwriter') as writer:
            with engine.connect() as connection:
                for table_name in table_names:
                    query_check_table = text("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = :table_name)")
                    if connection.execute(query_check_table, {'table_name': table_name}).scalar():
                        df = pd.read_sql_query(text(f"SELECT * FROM {table_name}"), connection)
                        formatted_df = format_dataframe_for_excel(df.copy(), table_name)
                        formatted_df.to_excel(writer, sheet_name=table_name, index=False)
                        
                        worksheet = writer.sheets[table_name]
                        for i, col in enumerate(formatted_df.columns):

                            if not formatted_df[col].empty:
                                max_len = formatted_df[col].astype(str).map(len).max()
                            else:
                                max_len = 0
                            column_len = max(max_len, len(col)) + 2
                            worksheet.set_column(i, i, column_len)

        await context.bot.send_document(chat_id=user_id, document=open(formatted_file_path, 'rb'), filename=f"Полная_выгрузка_БД_формат_{current_date_str}.xlsx")
        
        await show_main_menu_logic(context, user_id, chat_id, wait_msg.message_id, greeting="✅ Полный экспорт завершен.")

    except Exception as e:
        logger.error(f"Ошибка при полном экспорте БД: {e}")
        await wait_msg.edit_text("❌ Произошла ошибка при формировании файла.")
    finally:
        if raw_file_path and os.path.exists(raw_file_path): os.remove(raw_file_path)
        if formatted_file_path and os.path.exists(formatted_file_path): os.remove(formatted_file_path)

def format_dataframe_for_excel(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Приводит DataFrame в читаемый вид с учетом специфики каждой таблицы."""
    
    rename_map = {
        'id': 'ID', 'timestamp': 'Время создания', 'corpus_name': 'Корпус',
        'discipline_name': 'Дисциплина', 'work_type_name': 'Вид работ',
        'foreman_name': 'Бригадир', 'people_count': 'Кол-во чел.', 'volume': 'Объем',
        'report_date': 'Дата работ', 'kiok_approved': 'Статус КИОК',
        'kiok_approver_id': 'ID согласующего', 'kiok_approval_timestamp': 'Время согласования',
        'group_message_id': 'ID сообщения', 'user_id': 'UserID',
        'first_name': 'Имя', 'last_name': 'Фамилия', 'username': 'Username',
        'phone_number': 'Телефон', 'level': 'Уровень', 'brigade_name': 'Название бригады',
        'name': 'Название', 'discipline_id': 'ID Дисциплины', 'chat_id': 'ID Чата',
        'topic_id': 'ID Темы'
    }
    df.rename(columns=rename_map, inplace=True, errors='ignore')

    if table_name == 'reports':
        if 'Статус КИОК' in df.columns:
            status_map = {0: 'Ожидает', 1: 'Согласовано', -1: 'Отклонено'}
            df['Статус КИОК'] = df['Статус КИОК'].map(status_map).fillna('Неизвестно')

        timezone_aware_columns = ['Время создания', 'Время согласования']
        for col in timezone_aware_columns:
            if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
                if df[col].dt.tz is not None:
                     df[col] = df[col].dt.tz_localize(None)

        date_columns = ['Время создания', 'Время согласования']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d.%m.%Y %H:%M').fillna('')
    
    return df
 
async def handle_directories_excel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает загруженный Excel-файл, добавляя новые записи в справочники PostgreSQL."""

    excel_mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if not update.message.document or update.message.document.mime_type != excel_mime_type:
        return 

    user_id = str(update.effective_user.id)
    user_role = check_user_role(user_id)
    if not user_role.get('isAdmin'):
        await update.message.reply_text("⛔️ У вас нет прав для выполнения этой операции.")
        return
        
    await update.message.reply_text("✅ Файл получен. Начинаю обработку справочников...")
    
    file = await context.bot.get_file(update.message.document.file_id)
    file_path = os.path.join(TEMP_DIR, f"upload_{file.file_id}.xlsx")
    await file.download_to_drive(file_path)

    counters = {'disciplines': 0, 'objects': 0, 'work_types': 0}
    conn = None
    xls = None

    try:
        xls = pd.ExcelFile(file_path)
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cursor = conn.cursor()

        if 'Дисциплины' in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name='Дисциплины').dropna(subset=['name'])
            added_count = 0
            for name in df['name']:

                cursor.execute("INSERT INTO disciplines (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (str(name).strip(),))
                if cursor.rowcount > 0:
                    added_count += 1
            counters['disciplines'] = added_count

        if 'Корпуса' in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name='Корпуса').dropna(subset=['name'])

            cursor.execute("TRUNCATE TABLE construction_objects RESTART IDENTITY CASCADE;")
            for idx, name in enumerate(df['name']):
                cursor.execute("INSERT INTO construction_objects (name, display_order) VALUES (%s, %s)", (str(name).strip(), idx))
            counters['objects'] = len(df)

        if 'Виды работ' in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name='Виды работ').dropna(subset=['name', 'discipline_name'])
            cursor.execute("TRUNCATE TABLE work_types RESTART IDENTITY CASCADE;")
            
            cursor.execute("SELECT id, name FROM disciplines")
            disciplines_map = {name.upper(): disc_id for disc_id, name in cursor.fetchall()}
            
            added_count = 0
            for index, row in df.iterrows():
                work_name = str(row['name']).strip()
                discipline_name = str(row.get('discipline_name', '')).strip()
                unit = str(row.get('unit_of_measure', '')).strip()
                norm = row.get('norm_per_unit', 0.0)
                
                discipline_id = disciplines_map.get(discipline_name.upper())

                if discipline_id:
                    cursor.execute(
                        "INSERT INTO work_types (name, discipline_id, unit_of_measure, norm_per_unit, display_order) VALUES (%s, %s, %s, %s, %s)",
                        (work_name, discipline_id, unit, float(norm), index)
                    )
                    if cursor.rowcount > 0:
                        added_count += 1
                else:
                    logger.warning(f"Дисциплина '{discipline_name}' для вида работ '{work_name}' не найдена. Строка пропущена.")
            counters['work_types'] = added_count

        conn.commit()
        
        summary_text = (
            f"✅ Обработка файла завершена.\n\n"
            f"Обновлено записей:\n"
            f"  ▪️ Дисциплины: *{counters['disciplines']}* (добавлено новых)\n"
            f"  ▪️ Корпуса: *{counters['objects']}* (полностью перезаписано)\n"
            f"  ▪️ Виды работ: *{counters['work_types']}* (полностью перезаписано)"
        )
        await update.message.reply_text(summary_text, parse_mode="Markdown")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при обработке Excel-файла со справочниками: {e}")
        await update.message.reply_text("❌ Произошла ошибка при чтении или обработке файла. Убедитесь, что структура, названия листов и колонок верны.")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if xls:
            xls.close()
        if os.path.exists(file_path):
            os.remove(file_path)

# --- Редактирование пользователей от админа ---
async def show_user_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает меню с опциями для редактирования с учетом прав доступа."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    role, user_id_to_edit = parts[2], parts[3]

    viewer_id = str(query.from_user.id)
    viewer_role = check_user_role(viewer_id)
    
    user_data = db_query(f"SELECT first_name, last_name FROM {role} WHERE user_id = %s", (user_id_to_edit,))
    full_name = f"{user_data[0][0]} {user_data[0][1]}" if user_data else user_id_to_edit

    message_text = f"👤 *Редактирование: {full_name}*\n`{user_id_to_edit}`\n\nВыберите действие:"

    keyboard_buttons = []
    
    if role == 'managers':
        keyboard_buttons.append([InlineKeyboardButton("Изменить уровень", callback_data=f"change_level_{user_id_to_edit}")])
        keyboard_buttons.append([InlineKeyboardButton("Изменить дисциплину", callback_data=f"change_discipline_{role}_{user_id_to_edit}")])
    elif role in ['pto', 'kiok']:
        keyboard_buttons.append([InlineKeyboardButton("Изменить дисциплину", callback_data=f"change_discipline_{role}_{user_id_to_edit}")])
    elif role == 'brigades':
        keyboard_buttons.append([InlineKeyboardButton("Изменить дисциплину", callback_data=f"change_discipline_{role}_{user_id_to_edit}")])

        if viewer_role.get('isAdmin') or viewer_role.get('managerLevel') == 2 or viewer_role.get('isPto'):
            keyboard_buttons.append([InlineKeyboardButton("🔄 Сбросить сегодняшний табель", callback_data=f"reset_roster_{user_id_to_edit}")])

    if viewer_role.get('isAdmin') and viewer_id != user_id_to_edit and user_id_to_edit != OWNER_ID:
         keyboard_buttons.append([InlineKeyboardButton("🗑️ Удалить пользователя", callback_data=f"delete_user_{role}_{user_id_to_edit}")])
    
    keyboard_buttons.append([InlineKeyboardButton("◀️ Назад к списку", callback_data=f"list_users_{role}_1")])

    await query.edit_message_text(
        text=message_text,
        reply_markup=InlineKeyboardMarkup(keyboard_buttons),
        parse_mode="Markdown"
    )

async def show_discipline_change_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает меню для смены дисциплины пользователя."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    role, user_id_to_edit = parts[2], parts[3]

    disciplines_list = db_query("SELECT id, name FROM disciplines")
    
    if not disciplines_list:
        await query.edit_message_text("⚠️ В базе данных не найдено ни одной дисциплины.")
        return

    keyboard_buttons = []
    for discipline_id, discipline_name in disciplines_list:
        callback = f"set_discipline_{role}_{user_id_to_edit}_{discipline_id}"
        keyboard_buttons.append([InlineKeyboardButton(discipline_name, callback_data=callback)])
    
    keyboard_buttons.append([InlineKeyboardButton("◀️ Назад", callback_data=f"edit_user_{role}_{user_id_to_edit}")])

    await query.edit_message_text(
        text=f"Выберите новую дисциплину для пользователя `{user_id_to_edit}`:",
        reply_markup=InlineKeyboardMarkup(keyboard_buttons),
        parse_mode="Markdown"
    )

async def show_level_change_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает меню для смены уровня руководителя."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    user_id_to_edit = parts[2]

    keyboard_buttons = [
        [InlineKeyboardButton("Уровень 1 (полный доступ)", callback_data=f"set_level_{user_id_to_edit}_1")],
        [InlineKeyboardButton("Уровень 2 (по дисциплине)", callback_data=f"set_level_{user_id_to_edit}_2")],
        [InlineKeyboardButton("◀️ Назад", callback_data=f"edit_user_managers_{user_id_to_edit}")]
    ]

    await query.edit_message_text(
        text=f"Выберите новый уровень для руководителя `{user_id_to_edit}`:",
        reply_markup=InlineKeyboardMarkup(keyboard_buttons),
        parse_mode="Markdown"
    )

async def set_level(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Изменяет уровень руководителя и показывает админу простое подтверждение."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    user_id_to_edit, new_level = parts[2], int(parts[3])
    
    if new_level == 1:
        db_query("UPDATE managers SET level = 1, discipline = NULL WHERE user_id = %s", (user_id_to_edit,))
        
        try:
            context._application.user_data.pop(int(user_id_to_edit), None)
            logger.info(f"Состояние для пользователя {user_id_to_edit} было полностью сброшено из-за смены уровня на 1.")
            greeting_text = "⚙️ Администратор изменил ваш уровень руководства на «Уровень 1»."
            await force_user_to_main_menu(context, user_id_to_edit, greeting_text)
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {user_id_to_edit} о смене уровня. Ошибка: {e}")

        keyboard = [[InlineKeyboardButton("◀️ Назад к списку", callback_data="list_users_managers_1")]]
        await query.edit_message_text(
            text="✅ Уровень руководителя изменен на 1.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ConversationHandler.END
    
    elif new_level == 2:
        context.user_data['edit_user_id'] = user_id_to_edit
        disciplines = db_query("SELECT id, name FROM disciplines ORDER BY name")
        if not disciplines:
            await query.edit_message_text("Ошибка: нет дисциплин для назначения.")
            return ConversationHandler.END
        
        keyboard_btns = [[InlineKeyboardButton(name, callback_data=f"set_new_disc_{disc_id}")] for disc_id, name in disciplines]
        keyboard_btns.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_admin_action")])
        
        await query.edit_message_text(
            text=f"Теперь выберите дисциплину для руководителя `{user_id_to_edit}` (Уровень 2):",
            reply_markup=InlineKeyboardMarkup(keyboard_btns),
            parse_mode="Markdown"
        )
        return AWAITING_DISCIPLINE_FOR_MANAGER

async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Назначает нового администратора ответом на его сообщение."""
    user_id = str(update.effective_user.id)

    if user_id != OWNER_ID:
        await update.message.reply_text("⛔️ Эта команда доступна только создателю бота.")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Пожалуйста, используйте эту команду как ответ на сообщение пользователя, которого хотите назначить администратором.")
        return

    target_user = update.message.reply_to_message.from_user
    target_user_id = str(target_user.id)

    new_admin_info = {
        'first_name': target_user.first_name,
        'last_name': target_user.last_name or '',
        'username': target_user.username,
        'phone_number': '' 
    }

    for role_table in ['managers', 'brigades', 'pto', 'kiok']:
        user_data = db_query(f"SELECT phone_number FROM {role_table} WHERE user_id = %s", (target_user_id,))
        if user_data and user_data[0][0]:
            new_admin_info['phone_number'] = user_data[0][0]
            break

    update_user_role(target_user_id, 'admin', new_admin_info)
    
    await update.message.reply_text(
        f"✅ Пользователь *{new_admin_info['first_name']} {new_admin_info['last_name']}* (`{target_user_id}`) успешно назначен администратором.",
        parse_mode="Markdown"
    )

async def delete_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет пользователя, уведомляет его, и показывает админу простое подтверждение."""
    query = update.callback_query
    await query.answer("Удаляю...", show_alert=False)
    
    parts = query.data.split('_')
    role_to_delete, user_id_to_delete = parts[2], parts[3]

    db_query(f"DELETE FROM {role_to_delete} WHERE user_id = %s", (user_id_to_delete,))
    logger.info(f"Пользователь {user_id_to_delete} удален из роли {role_to_delete} администратором {query.from_user.id}")

    try:

        if int(user_id_to_delete) in context._application.user_data:
            context._application.user_data[int(user_id_to_delete)].clear()
            logger.info(f"Состояние для пользователя {user_id_to_delete} было полностью сброшено администратором.")
        
        greeting_text = "⚠️ Ваша роль была удалена администратором. Для дальнейшей работы пройдите авторизацию заново."

        await show_main_menu_logic(context, user_id_to_delete, int(user_id_to_delete), greeting=greeting_text)
        
    except Exception as e:
        logger.error(f"Не удалось уведомить пользователя {user_id_to_delete} об удалении. Возможно, бот заблокирован. Ошибка: {e}")

    keyboard = [[InlineKeyboardButton(f"◀️ Назад к списку", callback_data=f"list_users_{role_to_delete}_1")]]
    await query.edit_message_text(
        text=f"✅ Пользователь `{user_id_to_delete}` удален.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def set_new_discipline_for_manager(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Завершает смену уровня на 2, назначая дисциплину и сбрасывая состояние."""
    query = update.callback_query
    await query.answer()

    new_discipline_id = int(query.data.split('_')[-1])
    user_id_to_edit = context.user_data.get('edit_user_id')
    
    if not user_id_to_edit:

        await query.edit_message_text("❌ Ошибка: сессия истекла. Попробуйте снова.")
        return ConversationHandler.END

    db_query("UPDATE managers SET level = 2, discipline = %s WHERE user_id = %s", (new_discipline_id, user_id_to_edit))

    context._application.user_data.pop(int(user_id_to_edit), None)
    logger.info(f"Состояние для пользователя {user_id_to_edit} было полностью сброшено из-за смены уровня на 2.")

    greeting_text = "⚙️ Администратор присвоил вам Уровень 2 и назначил новую дисциплину."
    await force_user_to_main_menu(context, user_id_to_edit, greeting_text)

    discipline_name_raw = db_query("SELECT name FROM disciplines WHERE id = %s", (new_discipline_id,))
    new_discipline_name = discipline_name_raw[0][0] if discipline_name_raw else "Неизвестно"

    await query.edit_message_text(
        f"✅ Руководителю `{user_id_to_edit}` присвоен *Уровень 2* и дисциплина «*{new_discipline_name}*»."
    )

    context.user_data.clear()
    return ConversationHandler.END

async def cancel_admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет админское действие и возвращает в меню."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Действие отменено.")
    await manage_users_menu(update, context) 
    context.user_data.clear()
    return ConversationHandler.END

# --- Доп функции - Формирование отчета ---
async def get_corpus_and_ask_work_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает выбранный корпус, сохраняет его и показывает список видов работ для дисциплины пользователя."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split('_')
    selected_corps_id = parts[2] 

    corps_name_raw = db_query("SELECT name FROM construction_objects WHERE id = %s", (selected_corps_id,))
    if not corps_name_raw:
        await query.edit_message_text(text="⚠️ *Ошибка:* Выбранный корпус не найден. Обратитесь к администратору.")
        return ConversationHandler.END
    selected_corps_name = corps_name_raw[0][0]

    context.user_data['report_data']['corps_name'] = selected_corps_name
    context.user_data['report_creation_state'] = 'GETTING_WORK_TYPE' 

    await show_work_types_page(update, context, page=1)

    return GETTING_WORK_TYPE

async def show_work_types_page(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
    """Отображает страницу выбора вида работ"""
    
    query = update.callback_query
    chat_id = query.message.chat_id
    message_id_to_edit = query.message.message_id
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)

    discipline_name = context.user_data.get('report_data', {}).get('discipline_name')
    
    if not discipline_name:
        user_role = check_user_role(user_id)
        discipline_name = user_role.get('discipline')

    if not discipline_name:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id_to_edit, text="⚠️ *Ошибка:* Не удалось определить вашу дисциплину. Обратитесь к администратору.", parse_mode='Markdown')
        return ConversationHandler.END

    work_types_raw = db_query("""
     SELECT wt.id, wt.name, wt.unit_of_measure FROM work_types wt
     JOIN disciplines d ON wt.discipline_id = d.id
     WHERE d.name = %s
     ORDER BY wt.display_order, wt.name
     """, (discipline_name,))

    if not work_types_raw:
        text = get_text('report_error_no_work_types', lang).format(discipline=get_data_translation(discipline_name, lang))
        user_role_check = check_user_role(user_id)
        back_callback = "new_report" if (user_role_check.get('isAdmin') or user_role_check.get('managerLevel') == 1) else "back_to_start_report"
        keyboard = [[InlineKeyboardButton(get_text('back_button', lang), callback_data=back_callback)]]
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id_to_edit, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return ConversationHandler.END

    total_works = len(work_types_raw)
    total_pages = math.ceil(total_works / ELEMENTS_PER_PAGE) if total_works > 0 else 1
    start_index = (page - 1) * ELEMENTS_PER_PAGE
    end_index = start_index + ELEMENTS_PER_PAGE
    works_on_page = work_types_raw[start_index:end_index]

    keyboard_buttons = []

    for work_id, work_name_from_db, unit_from_db in works_on_page:
        # 1. Очищаем название работы от возможной единицы измерения
        clean_work_name = work_name_from_db.strip()
        if ',' in clean_work_name:
            # Отсекаем все, что после последней запятой
            clean_work_name = clean_work_name.rsplit(',', 1)[0].strip()

        # 2. Переводим чистое название
        translated_name = get_data_translation(clean_work_name, lang)
        
        button_text = translated_name 

        # 3. Если в отдельном столбце есть единица измерения, переводим и ее
        if unit_from_db and unit_from_db.strip():
            # 4. Соединяем переведенные части
            translated_unit = get_data_translation(unit_from_db.strip(), lang)
            button_text += f", {translated_unit}"

        keyboard_buttons.append([InlineKeyboardButton(button_text, callback_data=f"report_work_{work_id}")])

    navigation_buttons = []
    if page > 1:
        navigation_buttons.append(InlineKeyboardButton(get_text('back_button', lang), callback_data=f"paginate_work_types_{page - 1}"))
    if page < total_pages:
        navigation_buttons.append(InlineKeyboardButton(get_text('forward_button', lang), callback_data=f"paginate_work_types_{page + 1}"))
    if navigation_buttons:
        keyboard_buttons.append(navigation_buttons)
    
    user_role_check = check_user_role(user_id)
    back_button_callback = "new_report" if (user_role_check.get('isAdmin') or user_role_check.get('managerLevel') == 1) else "back_to_start_report"
    keyboard_buttons.append([InlineKeyboardButton(get_text('back_button', lang), callback_data=back_button_callback)])

    keyboard = InlineKeyboardMarkup(keyboard_buttons)
    
    title_text = get_text('report_step2_work_type', lang).format(discipline=discipline_name)
    page_info_text = get_text('page_of', lang).format(page=page, total_pages=total_pages)
    text = f"*{title_text}* ({page_info_text})"

    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id_to_edit,
        text=text,
        reply_markup=keyboard,
        parse_mode='Markdown'
    )

async def get_work_type_and_ask_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает вид работ и запрашивает количество человек."""
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)

    selected_work_type_id = query.data.split('_')[2]
   
    work_type_info_raw = db_query("SELECT name, unit_of_measure FROM work_types WHERE id = %s", (selected_work_type_id,))
    if not work_type_info_raw:
        await query.edit_message_text(text="⚠️ *Ошибка:* Выбранный вид работ не найден.")
        return ConversationHandler.END
    
    selected_work_type_name, unit_of_measure = work_type_info_raw[0]

    context.user_data['report_data']['work_type'] = selected_work_type_name
    context.user_data['report_data']['unit_of_measure'] = unit_of_measure
    
    keyboard = [[InlineKeyboardButton(get_text('back_button', lang), callback_data="back_to_ask_work_type")]]
    sent_message = await query.edit_message_text(
        text=f"*{get_text('report_step3_people_count', lang)}*",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    context.user_data['last_bot_message_id'] = sent_message.message_id

    return GETTING_PEOPLE_COUNT

async def get_people_count_and_ask_volume(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает кол-во человек, ПРОВЕРЯЕТ ОСТАТОК и решает, куда идти дальше ."""
    chat_id = update.effective_chat.id
    user_id = str(update.effective_user.id)
    lang = get_user_language(user_id)
    user_role = check_user_role(user_id)
    people_count_text = update.message.text
    
    last_bot_message_id = context.user_data.pop('last_bot_message_id', None)
    if last_bot_message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=last_bot_message_id)
        except Exception: pass
    await update.message.delete()

    try:
        people_count = int(people_count_text)
        if people_count <= 0:
            raise ValueError()
    except ValueError:
        error_text = get_text('report_error_invalid_number', lang)
        sent_message = await context.bot.send_message(chat_id, error_text, parse_mode="Markdown")
        context.user_data['last_bot_message_id'] = sent_message.message_id
        return GETTING_PEOPLE_COUNT

    today_str = date.today().strftime('%Y-%m-%d')
    roster_info = db_query("SELECT total_people FROM daily_rosters WHERE brigade_user_id = %s AND roster_date = %s", (user_id, today_str))
    
    if not roster_info:
        keyboard = [[InlineKeyboardButton(get_text('main_menu_title', lang), callback_data="go_back_to_main_menu")]]
        await context.bot.send_message(chat_id, get_text('report_error_no_roster', lang), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        return ConversationHandler.END

    total_declared = roster_info[0][0]
    brigade_name_for_query = user_role.get('brigadeName') or f"Бригада пользователя {user_id}"
    assigned_info = db_query("SELECT SUM(people_count) FROM reports WHERE foreman_name = %s AND report_date = %s", (brigade_name_for_query, today_str))
    total_assigned = assigned_info[0][0] or 0 if assigned_info else 0
    available_pool = total_declared - total_assigned
    
    if people_count > available_pool:
        error_text = get_text('report_error_no_people', lang).format(count=people_count, reserve=available_pool)
        sent_message = await context.bot.send_message(chat_id, error_text, parse_mode="Markdown")
        context.user_data['last_bot_message_id'] = sent_message.message_id
        return GETTING_PEOPLE_COUNT
        
    context.user_data['report_data']['people_count'] = people_count
    
    work_type_name = context.user_data.get('report_data', {}).get('work_type', '')
    if 'Прочие' in work_type_name:
        context.user_data['report_data']['volume'] = 0.0
        keyboard = [
            [InlineKeyboardButton(get_text('today_button', lang), callback_data="set_date_today"), InlineKeyboardButton(get_text('yesterday_button', lang), callback_data="set_date_yesterday")],
            [InlineKeyboardButton(get_text('cancel_button', lang), callback_data="cancel_report")]
        ]
        sent_message = await context.bot.send_message(
            chat_id, f"*{get_text('report_step5_date', lang)}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        context.user_data['last_bot_message_id'] = sent_message.message_id
        return GETTING_DATE
        
    else:
        unit_of_measure = context.user_data['report_data'].get('unit_of_measure', '') 
        if unit_of_measure:
            volume_prompt = get_text('report_step4_volume', lang).format(unit=unit_of_measure)
        else:
            volume_prompt = get_text('report_step4_volume_no_unit', lang)

        keyboard = [[InlineKeyboardButton(get_text('back_button', lang), callback_data="back_to_ask_count")], [InlineKeyboardButton(get_text('cancel_button', lang), callback_data="cancel_report")]]
        
        sent_message = await context.bot.send_message(chat_id, f"*{volume_prompt}*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        context.user_data['last_bot_message_id'] = sent_message.message_id
        return GETTING_VOLUME

async def get_volume_and_ask_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает объем и запрашивает дату."""
    chat_id = update.effective_chat.id
    user_id = str(update.effective_user.id)
    lang = get_user_language(user_id)
    volume_text = update.message.text.replace(',', '.')
    
    last_bot_message_id = context.user_data.pop('last_bot_message_id', None)
    if last_bot_message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=last_bot_message_id)
        except Exception: pass
    await update.message.delete()

    try:
        volume = float(volume_text)
    except ValueError:
        error_text = get_text('report_error_invalid_volume', lang)
        keyboard = [[InlineKeyboardButton(get_text('back_button', lang), callback_data="back_to_ask_count")], [InlineKeyboardButton(get_text('cancel_button', lang), callback_data="cancel_report")]]
        sent_message = await context.bot.send_message(chat_id, error_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        context.user_data['last_bot_message_id'] = sent_message.message_id
        return GETTING_VOLUME

    context.user_data['report_data']['volume'] = volume
       
    keyboard = [
        [InlineKeyboardButton(get_text('back_button', lang), callback_data="back_to_ask_volume")],
        [InlineKeyboardButton(get_text('today_button', lang), callback_data="set_date_today"), InlineKeyboardButton(get_text('yesterday_button', lang), callback_data="set_date_yesterday")]
    ]
    text = f"*{get_text('report_step5_date', lang)}*"
    sent_message = await context.bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    context.user_data['last_bot_message_id'] = sent_message.message_id

    return GETTING_DATE

async def get_date_and_ask_notes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает дату и предлагает добавить примечание."""
    chat_id = update.effective_chat.id
    user_id = str(update.effective_user.id)
    lang = get_user_language(user_id)
    date_obj = None
    
    if update.callback_query: 
        query = update.callback_query
        await query.answer()
        await query.message.delete()
        
        if query.data == 'set_date_today':
            date_obj = date.today()
        elif query.data == 'set_date_yesterday':
            date_obj = date.today() - timedelta(days=1)
            
    else: 
        date_text = update.message.text
        last_bot_message_id = context.user_data.pop('last_bot_message_id', None)
        if last_bot_message_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_bot_message_id)
            except Exception: pass
        await update.message.delete()
                      
        try:
            date_obj = datetime.strptime(date_text, "%d.%m.%Y").date()
        except ValueError:
            error_text = get_text('report_error_invalid_date', lang)
            keyboard = [
                [InlineKeyboardButton(get_text('back_button', lang), callback_data="back_to_ask_volume")],
                [InlineKeyboardButton(get_text('today_button', lang), callback_data="set_date_today"), InlineKeyboardButton(get_text('yesterday_button', lang), callback_data="set_date_yesterday")]
            ]
            sent_message = await context.bot.send_message(chat_id, error_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            context.user_data['last_bot_message_id'] = sent_message.message_id
            return GETTING_DATE

    if date_obj:
        context.user_data['report_data']['report_date_db'] = date_obj.strftime("%Y-%m-%d")
        context.user_data['report_data']['report_date_display'] = date_obj.strftime("%d.%m.%Y")

        keyboard = [
            [InlineKeyboardButton(get_text('report_add_note_button', lang), callback_data="add_note")],
            [InlineKeyboardButton(get_text('skip_button', lang), callback_data="skip_note")],
            [InlineKeyboardButton(get_text('back_button', lang), callback_data="back_to_ask_date")]
        ]
        text = f"*{get_text('report_step6_notes_prompt', lang)}*"
        await context.bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        return GETTING_NOTES
    
    return GETTING_DATE
    
async def submit_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Собирает все данные, сохраняет отчет и отправляет уведомление."""
    query = update.callback_query
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)

    await query.answer()
    await query.edit_message_text(get_text('report_saving_and_sending', lang))

    report_data = context.user_data.get('report_data', {})
    user_role = check_user_role(user_id)
 
    discipline_name = report_data.get('discipline_name')
    
    if user_role.get('isAdmin') or user_role.get('managerLevel') == 1:
        foreman_name = f"Администратор ({query.from_user.first_name})"
    else: 
        foreman_name = user_role.get('brigadeName')

    if not discipline_name:

        await query.edit_message_text(get_text('report_error_no_discipline', lang))
        return ConversationHandler.END

    corpus_name = report_data.get('corps_name')
    work_type_name = report_data.get('work_type')
    people_count = report_data.get('people_count')
    volume = report_data.get('volume')
    report_date_db = report_data.get('report_date_db')
    notes = report_data.get('notes')

    report_id = db_query(
        """
        INSERT INTO reports (timestamp, corpus_name, discipline_name, work_type_name, foreman_name, people_count, volume, report_date, notes)
        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
        """,
        (corpus_name, discipline_name, work_type_name, foreman_name, people_count, volume, report_date_db, notes)
    )

    if not report_id:
        await query.edit_message_text(get_text('report_error_db_save', lang), parse_mode="Markdown")
        return ConversationHandler.END
        
    logger.info(f"Создан отчет в БД с ID: {report_id}")

    mapping = db_query("SELECT chat_id, topic_id FROM topic_mappings WHERE discipline_name ILIKE %s", (discipline_name,))
    
    if mapping:
        chat_id, topic_id = mapping[0]

        report_date_display = datetime.strptime(report_date_db, "%Y-%m-%d").strftime("%d.%m.%Y")
        unit_of_measure_raw = db_query("SELECT unit_of_measure FROM work_types WHERE name = %s", (work_type_name,))
        unit_of_measure = unit_of_measure_raw[0][0] if unit_of_measure_raw and unit_of_measure_raw[0][0] else ""

        report_lines = [
            f"📄 *Новый отчет от бригадира: {foreman_name}* (ID: {report_id})\n",
            f"▪️ *Корпус:* {corpus_name}",
            f"▪️ *Дисциплина:* {discipline_name}",
            f"▪️ *Вид работ:* {work_type_name}",
            f"▪️ *Дата:* {report_date_display}",
            f"▪️ *Кол-во человек:* {people_count}",
            f"▪️ *Выполненный объем:* {volume} {unit_of_measure}"
        ]
        if notes:
            report_lines.append(f"▪️ *Примечание:* {notes}")
        
        report_lines.append(f"\n*Статус:* ⏳ Ожидает согласования КИОК")
        report_text = "\n".join(report_lines)
        
        keyboard = [[
            InlineKeyboardButton("✅ Согласовать", callback_data=f"kiok_approve_{report_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"kiok_reject_{report_id}")
        ]]
        
        try:
            sent_message_in_group = await context.bot.send_message(
                chat_id=chat_id, text=report_text, message_thread_id=topic_id,
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
            )
            db_query("UPDATE reports SET group_message_id = %s WHERE id = %s", (sent_message_in_group.message_id, report_id))
            
            await query.answer(get_text('report_saved_and_sent', lang), show_alert=True)
            await show_main_menu_logic(
              context=context,
              user_id=user_id,
              chat_id=query.message.chat_id,
              message_id_to_edit=query.message.message_id,
              greeting=get_text('report_accepted_greeting', lang)
             )

        except Exception as e:
            logger.error(f"Не удалось отправить отчет в группу: {e}")

            await query.edit_message_text(get_text('report_error_group_send', lang), parse_mode="Markdown")

    else:
        error_text = get_text('report_saved_not_sent_error', lang).format(discipline=discipline_name)
        keyboard = [[InlineKeyboardButton(get_text('main_menu_title', lang), callback_data="go_back_to_main_menu")]]
        await query.edit_message_text(error_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            
    context.user_data.clear()
    return ConversationHandler.END

async def get_directories_template(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Формирует и отправляет админу Excel-файл-шаблон для заполнения справочников."""
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)

    user_role = check_user_role(user_id)
    if not user_role.get('isAdmin'):
        await query.answer("⛔️ У вас нет прав для этого действия.", show_alert=True)
        return

    await query.edit_message_text("⏳ Создаю файл-шаблон для справочников...")
    
    file_path = None
    try:
        current_date_str = date.today().strftime('%Y-%m-%d')
        file_path = os.path.join(TEMP_DIR, f"template_directories_{current_date_str}.xlsx")
        
        engine = create_engine(DATABASE_URL)

        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            with engine.connect() as connection:

                df_disciplines = pd.read_sql_query(text("SELECT name FROM disciplines"), connection)
                df_objects = pd.read_sql_query(text("SELECT name, display_order FROM construction_objects ORDER BY display_order"), connection)
                
                query_work_types = """
                    SELECT wt.name, d.name as discipline_name, wt.unit_of_measure, wt.norm_per_unit
                    FROM work_types wt
                    JOIN disciplines d ON wt.discipline_id = d.id
                    ORDER BY d.name, wt.display_order
                """
                df_work_types = pd.read_sql_query(text(query_work_types), connection)

                df_disciplines.to_excel(writer, sheet_name='Дисциплины', index=False)
                df_objects.to_excel(writer, sheet_name='Корпуса', index=False)
                df_work_types.to_excel(writer, sheet_name='Виды работ', index=False)

        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=open(file_path, 'rb'),
            filename="Шаблон_справочников.xlsx",
            caption="📄 Вот шаблон с текущими данными. Отредактируйте его и отправьте обратно, чтобы обновить справочники."
        )
        await query.message.delete()

    except Exception as e:
        logger.error(f"Ошибка при создании шаблона справочников: {e}")
        await query.message.reply_text("❌ Произошла ошибка при создании файла-шаблона.")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

async def generate_discipline_personnel_report(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                                discipline_name: str = None, start_date: str = None, end_date: str = None,
                                                period_display_text: str = None) -> None:
    """
    Генерирует ПРОСТУЮ ИТОГОВУЮ СВОДКУ по персоналу для дисциплины и периода.
    """
    query = update.callback_query
    await query.answer()

    if discipline_name is None:
        parts = query.data.split('_')
        if parts[0] == 'ph' and parts[1] == 's':
            discipline_name = parts[2]
            start_date = parts[3]
            end_date = parts[4]
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            if start_date_obj == end_date_obj:
                period_display_text = f"на {start_date_obj.strftime('%d.%m.%Y')}"
            else:
                period_display_text = f"за период с {start_date_obj.strftime('%d.%m.%Y')} по {end_date_obj.strftime('%d.%m.%Y')}"

    await query.edit_message_text(f"⏳ Собираю сводку для «{discipline_name}» {period_display_text}...")

    try:

        discipline_id_raw = db_query("SELECT id FROM disciplines WHERE name = %s", (discipline_name,))
        if not discipline_id_raw:
            await query.edit_message_text(f"❌ Ошибка: Дисциплина «{discipline_name}» не найдена.")
            return
        discipline_id = discipline_id_raw[0][0]

        summary_query = """
            SELECT
                pr.role_name,
                SUM(drd.people_count) as total_by_role
            FROM daily_roster_details drd
            JOIN daily_rosters dr ON drd.roster_id = dr.id
            JOIN personnel_roles pr ON drd.role_id = pr.id
            WHERE dr.roster_date BETWEEN %s AND %s AND pr.discipline_id = %s
            GROUP BY pr.role_name
            ORDER BY pr.role_name;
        """
        summary_data = db_query(summary_query, (start_date, end_date, discipline_id))

        brigades_count_query = """
            SELECT COUNT(DISTINCT dr.brigade_user_id)
            FROM daily_rosters dr
            JOIN brigades b ON dr.brigade_user_id = b.user_id
            WHERE dr.roster_date BETWEEN %s AND %s AND b.discipline = %s
        """
        brigades_count_raw = db_query(brigades_count_query, (start_date, end_date, discipline_id))
        brigades_count = brigades_count_raw[0][0] if brigades_count_raw else 0

        message_lines = [f"📊 *Сводка по персоналу: «{discipline_name}»*\n_{period_display_text}_", ""]

        if not summary_data:
            message_lines.append("Нет данных за выбранный период.")
        else:
            total_people = sum(item[1] for item in summary_data)
            message_lines.append(f"▪️ *Всего заявлено:* {total_people} чел.")
            message_lines.append(f"▪️ *Активных бригад:* {brigades_count}")
            message_lines.append("\n*Состав по должностям:*")
            role_lines = [f"  - {role}: *{count}* чел." for role, count in summary_data]
            message_lines.extend(role_lines)

        keyboard = [[InlineKeyboardButton("◀️ Назад к выбору периода", callback_data=f"personnel_history_discipline_select_{discipline_id}")]]
        await query.edit_message_text("\n".join(message_lines), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Ошибка при генерации сводки по персоналу: {e}")
        await query.edit_message_text("❌ Произошла ошибка при формировании отчета.")

async def handle_problem_brigades_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Показывает меню выбора даты для отчета 'Проблемные бригады'.
    """
    query = update.callback_query
    await query.answer()
    lang = get_user_language(str(query.from_user.id))

    keyboard = [
        [InlineKeyboardButton("Сегодня", callback_data="problem_brigades_by_date_today")],
        [InlineKeyboardButton("Вчера", callback_data="problem_brigades_by_date_yesterday")],
        [InlineKeyboardButton("◀️ Назад", callback_data="report_menu_all")]
    ]
    await query.edit_message_text(
        text="*⚠️ Проблемные бригады*\n\nВыберите период для просмотра отчета:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

# --- Доп функции - Исторический отчет табель ---

async def show_hr_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает меню 'Людские ресурсы', ИСПОЛЬЗУЯ ID ДИСЦИПЛИН в кнопках."""
    query = update.callback_query
    await query.answer()

    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)
    user_role = check_user_role(user_id)

    await query.edit_message_text(f"⏳ {get_text('loading_please_wait', lang)}")

    today_str = date.today().strftime('%Y-%m-%d')
    
    base_query = """
        SELECT SUM(dr.total_people), d.name, COUNT(DISTINCT dr.brigade_user_id)
        FROM daily_rosters dr
        JOIN brigades b ON dr.brigade_user_id = b.user_id
        JOIN disciplines d ON b.discipline = d.id 
        WHERE dr.roster_date = %s
    """
    params = [today_str]

    if not (user_role.get('isAdmin') or user_role.get('managerLevel') == 1):
        user_discipline = user_role.get('discipline')
        if user_discipline:
            base_query += " AND d.name = %s"
            params.append(user_discipline)

    base_query += " GROUP BY d.name ORDER BY d.name"
    summary_data = db_query(base_query, tuple(params))

    title = get_text('hr_summary_title', lang)
    message_lines = [f"*{title}*"]
    if not summary_data:
        message_lines.append(f"\n_{get_text('no_rosters_today', lang)}_")
    else:
        total_people = sum(item[0] for item in summary_data)
        message_lines.append(f"\n{get_text('total_people_today', lang).format(total=total_people)}")
        message_lines.append("\n*В разрезе дисциплин (состав):*")
        for total, disc_name, brigade_count in summary_data:
            roles_q = db_query("""
                SELECT pr.role_name, SUM(drd.people_count) as count FROM daily_roster_details drd
                JOIN daily_rosters dr ON drd.roster_id = dr.id
                JOIN personnel_roles pr ON drd.role_id = pr.id
                JOIN brigades b ON dr.brigade_user_id = b.user_id
                JOIN disciplines d ON b.discipline = d.id
                WHERE dr.roster_date = %s AND d.name = %s GROUP BY pr.role_name ORDER BY count DESC LIMIT 3
            """, (today_str, disc_name))
            roles_text = ", ".join([f"{r_name}: {r_count}" for r_name, r_count in roles_q]) if roles_q else "нет данных"
            message_lines.append(f"  - *{get_data_translation(disc_name, lang)}:* {total} чел. ({brigade_count} бр.)\n    `({roles_text})`")

    message_lines.append(f"\n\n*{get_text('hr_discipline_select_prompt', lang)}*")

    keyboard = []

    if user_role.get('isAdmin') or user_role.get('managerLevel') == 1:
        disciplines_for_buttons = db_query("SELECT id, name FROM disciplines ORDER BY name")
        if disciplines_for_buttons:
             disc_buttons = [InlineKeyboardButton(get_data_translation(d_name, lang), callback_data=f"hr_report_today_{d_id}_1") for d_id, d_name in disciplines_for_buttons]
             keyboard.extend([disc_buttons[i:i + 2] for i in range(0, len(disc_buttons), 2)])
    else:
        user_discipline = user_role.get('discipline')
        if user_discipline:
            discipline_id_raw = db_query("SELECT id FROM disciplines WHERE name = %s", (user_discipline,))
            if discipline_id_raw:
                user_discipline_id = discipline_id_raw[0][0]
                keyboard.append([InlineKeyboardButton(get_data_translation(user_discipline, lang), callback_data=f"hr_report_today_{user_discipline_id}_1")])

    keyboard.append([InlineKeyboardButton(get_text('back_button', lang), callback_data="report_menu_all")])
    await query.edit_message_text("\n".join(message_lines), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def show_paginated_brigade_report(update: Update, context: ContextTypes.DEFAULT_TYPE, start_date_override: date = None, end_date_override: date = None) -> None:
    """Финальный отчет по бригадам, ПРИНИМАЮЩИЙ ID ДИСЦИПЛИНЫ."""
    query = update.callback_query
    wait_msg = None  

    if query:

        user_id = str(query.from_user.id)
        lang = get_user_language(user_id)
        await query.answer()
        await query.edit_message_text(f"⏳ {get_text('loading_please_wait', lang)}")
        wait_msg = query.message
    else:

        user_id = str(update.effective_user.id)
        lang = get_user_language(user_id)
        await update.message.delete()

        wait_msg = await context.bot.send_message(chat_id=user_id, text=f"⏳ {get_text('loading_please_wait', lang)}")
    
    page = 1
    report_date = date.today()
    discipline_id = context.user_data.get('hr_discipline_filter')

    if query:
        parts = query.data.split('_')
        period = parts[2]
        discipline_id = int(parts[3])
        page = int(parts[4])
        context.user_data['hr_discipline_filter'] = discipline_id
        if period == 'yesterday': 
            report_date = date.today() - timedelta(days=1)
    
    if start_date_override: 
        report_date = start_date_override
    
    discipline_name_raw = db_query("SELECT name FROM disciplines WHERE id = %s", (discipline_id,))
    if not discipline_name_raw:
        await wait_msg.edit_text("Ошибка: Дисциплина не найдена по ID.")
        return
    discipline_name = discipline_name_raw[0][0]

    report_date_str = report_date.strftime('%Y-%m-%d')
    
    brigades_q = db_query(
        "SELECT b.brigade_name FROM daily_rosters dr JOIN brigades b ON dr.brigade_user_id = b.user_id WHERE dr.roster_date = %s AND b.discipline = %s ORDER BY b.brigade_name",
        (report_date_str, discipline_id)
    )
    all_brigades = [row[0] for row in brigades_q] if brigades_q else []
    
    items_per_page = 5
    total_pages = math.ceil(len(all_brigades) / items_per_page) if all_brigades else 1
    page = max(1, min(page, total_pages))
    start_index = (page - 1) * items_per_page
    brigades_on_page = all_brigades[start_index : start_index + items_per_page]

    brigade_reports = {}
    for b_name in brigades_on_page:
        details_q = db_query("""
            SELECT dr.total_people, pr.role_name, drd.people_count
            FROM daily_rosters dr
            JOIN brigades b ON dr.brigade_user_id = b.user_id
            JOIN daily_roster_details drd ON dr.id = drd.roster_id
            JOIN personnel_roles pr ON drd.role_id = pr.id
            WHERE dr.roster_date = %s AND b.brigade_name = %s
        """, (report_date_str, b_name))
        
        if details_q:
            brigade_reports[b_name] = {'total': details_q[0][0], 'roles': [f"  - {get_data_translation(role, lang)}: {count}" for _, role, count in details_q]}

    disc_summary_q = db_query("SELECT SUM(dr.total_people), COUNT(DISTINCT dr.brigade_user_id) FROM daily_rosters dr JOIN brigades b ON dr.brigade_user_id = b.user_id WHERE dr.roster_date = %s AND b.discipline = %s", (report_date_str, discipline_id))
    disc_total_people, disc_brigade_count = (disc_summary_q[0][0] or 0, disc_summary_q[0][1] or 0) if disc_summary_q else (0,0)

    title = get_text('hr_report_title', lang).format(discipline=get_data_translation(discipline_name, lang), date=report_date.strftime('%d.%m.%Y'))
    message_lines = [f"*{title}*", get_text('hr_report_summary', lang).format(total_people=disc_total_people, brigade_count=disc_brigade_count), "---"]

    if not brigade_reports:
        message_lines.append(f"_{get_text('no_rosters_for_period', lang).format(discipline=get_data_translation(discipline_name, lang))}_")
    else:
        for i, brigade_name in enumerate(brigades_on_page, start=start_index + 1):
            data = brigade_reports.get(brigade_name, {'total': 'N/A', 'roles': []})
            message_lines.append(f"\n*{i}. {get_data_translation(brigade_name, lang)}* ({get_text('total_declared', lang).format(total=data['total'])})")
            message_lines.extend(data['roles'])

    keyboard = []
    
    if isinstance(report_date, date) and report_date == date.today():
        current_period_for_callback = 'today'
    elif isinstance(report_date, date) and report_date == date.today() - timedelta(days=1):
        current_period_for_callback = 'yesterday'
    else:
        current_period_for_callback = report_date.strftime('%Y-%m-%d')

    pagination_buttons = []
    if page > 1: pagination_buttons.append(InlineKeyboardButton("◀️", callback_data=f"hr_report_{current_period_for_callback}_{discipline_id}_{page - 1}"))
    if page < total_pages: pagination_buttons.append(InlineKeyboardButton("▶️", callback_data=f"hr_report_{current_period_for_callback}_{discipline_id}_{page + 1}"))
    if pagination_buttons: keyboard.append(pagination_buttons)

    date_buttons = [
        InlineKeyboardButton(get_text('yesterday_button', lang), callback_data=f"hr_report_yesterday_{discipline_id}_1"),
        InlineKeyboardButton(get_text('today_button', lang), callback_data=f"hr_report_today_{discipline_id}_1"),
        InlineKeyboardButton(get_text('pick_date_button', lang), callback_data=f"hr_date_select_{discipline_id}")
    ]
    keyboard.append(date_buttons)
    keyboard.append([InlineKeyboardButton(get_text('back_button', lang), callback_data="hr_menu")])

    await context.bot.edit_message_text(
        chat_id=user_id,
        message_id=wait_msg.message_id,
        text="\n".join(message_lines), 
        reply_markup=InlineKeyboardMarkup(keyboard), 
        parse_mode="Markdown"
    )

async def get_hr_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запрашивает дату для отчета по персоналу, ПРИНИМАЯ ID ДИСЦИПЛИНЫ."""
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)

    discipline_id = query.data.split('_')[-1]
    context.user_data['hr_discipline_filter'] = int(discipline_id)

    message = await query.edit_message_text(get_text('history_prompt_date', lang), parse_mode="Markdown")
    context.user_data['last_bot_message_id'] = message.message_id
    return GETTING_HR_DATE

async def process_hr_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает введенную дату и вызывает отчет."""
    user_input = update.message.text.strip()
    try:
        date_obj = datetime.strptime(user_input, "%d.%m.%Y").date()
        await show_paginated_brigade_report(update, context, start_date_override=date_obj, end_date_override=date_obj)
    except (ValueError, IndexError):
        lang = get_user_language(str(update.effective_user.id))
        await update.message.reply_text(get_text('report_error_invalid_date', lang), parse_mode="Markdown")
        return 1 # Остаемся в том же состоянии

    context.user_data.clear()
    return ConversationHandler.END

# --- Доп функции - Формирование отчета бригадира ---
async def prompt_for_note(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запрашивает у пользователя текст примечания (МНОГОЯЗЫЧНАЯ ВЕРСИЯ)."""
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)
    
    text = get_text('report_note_prompt', lang)
    sent_message = await query.edit_message_text(text)
    context.user_data['last_bot_message_id'] = sent_message.message_id
    
    return GETTING_NOTES

async def get_note_and_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает текст примечания и переходит к подтверждению."""
    note_text = update.message.text
    context.user_data['report_data']['notes'] = note_text
    
    last_bot_message_id = context.user_data.pop('last_bot_message_id', None)
    if last_bot_message_id:
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=last_bot_message_id)
        except Exception: pass
    await update.message.delete()

    return await confirm_report_logic(update, context)

async def skip_note_and_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Пропускает добавление примечания и переходит к подтверждению."""
    query = update.callback_query
    await query.answer()
    context.user_data['report_data']['notes'] = None
    return await confirm_report_logic(update, context)

async def confirm_report_logic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отображает финальный отчет для подтверждения (МНОГОЯЗЫЧНАЯ ВЕРСИЯ)."""
    if update.callback_query:
        chat_id = update.callback_query.message.chat_id
        user_id = str(update.callback_query.from_user.id)
    else:
        chat_id = update.effective_chat.id
        user_id = str(update.effective_user.id)
    
    lang = get_user_language(user_id)
    data = context.user_data['report_data']
    
    summary_lines = [
        f"*{get_text('report_final_confirmation_title', lang)}*",
        f"{get_text('report_summary_corpus', lang)} {data.get('corps_name')}",
        f"{get_text('report_summary_work_type', lang)} {data.get('work_type')}",
        f"{get_text('report_summary_date', lang)} {data.get('report_date_display')}",
        f"{get_text('report_summary_people', lang)} {data.get('people_count')}",
        f"{get_text('report_summary_volume', lang)} {data.get('volume')} {data.get('unit_of_measure', '')}"
    ]
    
    if data.get('notes'):
        summary_lines.append(f"{get_text('report_summary_notes', lang)} {data.get('notes')}")

    summary_text = "\n".join(summary_lines)
    keyboard = [
        [InlineKeyboardButton(get_text('report_confirm_and_send_button', lang), callback_data="submit_report")],
        [InlineKeyboardButton(get_text('back_button', lang), callback_data="back_to_ask_date")]
    ]
    
    if update.callback_query:
        await update.callback_query.edit_message_text(summary_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id, summary_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

    return CONFIRM_REPORT

async def execute_dangerous_roster_save(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """УДАЛЯЕТ отчеты за день и затем сохраняет новый табель (МНОГОЯЗЫЧНАЯ ВЕРСИЯ)."""
    query = update.callback_query
    await query.answer()
    
    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)
    user_role = check_user_role(user_id)
    today_str = date.today().strftime('%Y-%m-%d')
    brigade_name = user_role.get('brigadeName') or f"Бригада пользователя {user_id}"

    db_query("DELETE FROM reports WHERE foreman_name = %s AND report_date = %s", (brigade_name, today_str))
    
    greeting_text = get_text('roster_force_saved_success', lang)
    return await execute_final_roster_save(update, context, greeting=greeting_text)

async def execute_final_roster_save(update: Update, context: ContextTypes.DEFAULT_TYPE, greeting: str) -> int:
    """Финальная стадия: сохраняет табель в БД и показывает главное меню."""
    query = update.callback_query
    user_id = str(query.from_user.id)
    roster_summary = context.user_data.get('roster_summary')
    today_str = date.today().strftime('%Y-%m-%d')
    total_people_new = roster_summary['total']

    # Удаляем старый табель, если он был (для чистоты)
    db_query("DELETE FROM daily_rosters WHERE brigade_user_id = %s AND roster_date = %s", (user_id, today_str))

    # Сохраняем "шапку" табеля
    roster_id = db_query(
        "INSERT INTO daily_rosters (roster_date, brigade_user_id, total_people) VALUES (%s, %s, %s) RETURNING id",
        (today_str, user_id, total_people_new)
    )
    
    # Сохраняем детализацию
    if roster_id:
        roles_map_raw = db_query("SELECT id, role_name FROM personnel_roles")
        roles_map = {name: role_id for role_id, name in roles_map_raw} if roles_map_raw else {}
        details_to_save = roster_summary.get('details', {})
        for role_name, count in details_to_save.items():
            role_id = roles_map.get(role_name)
            if role_id:
                db_query(
                    "INSERT INTO daily_roster_details (roster_id, role_id, people_count) VALUES (%s, %s, %s)",
                    (roster_id, role_id, count)
                )
        
        await query.edit_message_text(greeting, parse_mode="Markdown")
        await show_main_menu_logic(context, user_id, query.message.chat_id)
    else:
        await query.edit_message_text("❌ Произошла критическая ошибка при сохранении табеля.")

    context.user_data.clear()
    return ConversationHandler.END

def get_low_performance_brigade_count(discipline_name: str) -> int:
    """Возвращает количество бригад с выработкой ниже 100% по дисциплине."""
    today_str = date.today().strftime('%Y-%m-%d')
    engine = create_engine(DATABASE_URL)
    
    pd_query = """
        SELECT r.foreman_name, r.people_count, r.volume, wt.norm_per_unit, wt.name as work_type_name_alias
        FROM reports r 
        JOIN work_types wt ON r.work_type_name = wt.name AND r.discipline_name = (SELECT d.name FROM disciplines d WHERE d.id = wt.discipline_id)
        WHERE r.discipline_name = :discipline_name AND r.report_date = :today AND r.kiok_approved = 1
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(pd_query), connection, params={'discipline_name': discipline_name, 'today': today_str})

        if df.empty:
            return 0
            
        performance_df = df[~df['work_type_name_alias'].str.contains('Прочие', case=False, na=False)].copy()
        if performance_df.empty:
            return 0
            
        performance_df['planned_volume'] = pd.to_numeric(performance_df['people_count'], errors='coerce') * pd.to_numeric(performance_df['norm_per_unit'], errors='coerce')
        mask = performance_df['planned_volume'] > 0
        performance_df.loc[mask, 'output_percentage'] = (pd.to_numeric(performance_df.loc[mask, 'volume']) / performance_df.loc[mask, 'planned_volume']) * 100
        
        avg_performance = performance_df.groupby('foreman_name')['output_percentage'].mean()
        low_performers_series = avg_performance[avg_performance < 100]
        
        return len(low_performers_series)
    except Exception as e:
        logger.error(f"Ошибка при подсчете бригад с низкой выработкой для '{discipline_name}': {e}")
        return 0

# --- Пагинация формирование отчетов---

async def paginate_corps(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает нажатие кнопок пагинации для корпусов."""
    query = update.callback_query
    page = int(query.data.split('_')[-1])
    await show_corps_page(update, context, page=page)

async def paginate_work_types(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает нажатие кнопок пагинации для видов работ."""
    query = update.callback_query
    page = int(query.data.split('_')[-1])
    await show_work_types_page(update, context, page=page)

# Код для полной замены

async def show_foreman_performance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает отчет о выработке для бригадира."""
    query = update.callback_query
    await query.answer()

    user_id = str(query.from_user.id)
    lang = get_user_language(user_id)
    
    await query.edit_message_text(get_text('foreman_performance_analyzing', lang))

    user_role = check_user_role(user_id)
    brigade_name = user_role.get('brigadeName')

    if not brigade_name:
        await query.edit_message_text("❗*Ошибка:* Не удалось определить вашу бригаду.")
        return

    try:
        engine = create_engine(DATABASE_URL)
        query_text = """
            SELECT r.report_date, r.work_type_name, r.people_count, r.volume, wt.norm_per_unit, wt.unit_of_measure
            FROM reports r
            JOIN work_types wt ON r.work_type_name = wt.name AND r.discipline_name = (SELECT d.name FROM disciplines d WHERE d.id = wt.discipline_id)
            WHERE r.foreman_name = :brigade_name
            ORDER BY r.id DESC LIMIT 5
        """
        with engine.connect() as connection:
            reports_df = pd.read_sql_query(text(query_text), connection, params={'brigade_name': brigade_name})

        title = f"*{get_text('foreman_performance_title', lang)}*"
        
        if reports_df.empty:
            message_text = f"{title}\n\n_{get_text('foreman_performance_no_reports', lang)}_"
        else:
            reports_df['planned_volume'] = reports_df['people_count'] * reports_df['norm_per_unit']
            reports_df['output_percentage'] = (reports_df['volume'] / reports_df['planned_volume'].replace(0, 1)) * 100
            avg_performance = reports_df['output_percentage'].mean()
            
            message_lines = [
                title,
                f"{get_text('foreman_performance_avg_label', lang)} *{avg_performance:.1f}%*\n"
            ]
            
            for index, row in reports_df.iterrows():
                report_date_formatted = row['report_date'].strftime("%d.%m.%Y")

                work_type_translated = get_data_translation(row['work_type_name'], lang)
                
                message_lines.append(
                    f"*{report_date_formatted}* - {work_type_translated}\n"
                    f"  {get_text('foreman_performance_volume_label', lang)}: {row['volume']} {row['unit_of_measure']} / {get_text('foreman_performance_output_label', lang)}: *{row['output_percentage']:.1f}%*"
                )
            message_text = "\n".join(message_lines)

    except Exception as e:
        logger.error(f"Ошибка при создании отчета для бригадира: {e}")
        message_text = f"❌ {get_text('error_generic', lang)}"

    keyboard = [[InlineKeyboardButton(get_text('foreman_performance_back_button', lang), callback_data="report_menu_all")]]
    await query.edit_message_text(
        text=message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

# --- Доп функции - КИОК ---
async def handle_kiok_decision(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает решение КИОК, используя регистрационное имя и добавляя отладку."""
    query = update.callback_query
    
    parts = query.data.split('_')
    action = parts[1]
    report_id = int(parts[2])
    
    user_id = str(query.from_user.id)
    user_role = check_user_role(user_id)

    report_info_raw = db_query(
        "SELECT r.discipline_name, tm.chat_id, r.group_message_id, r.report_date, r.foreman_name, r.corpus_name, r.work_type_name, r.people_count, r.volume, r.notes "
        "FROM reports r LEFT JOIN topic_mappings tm ON r.discipline_name = tm.discipline_name WHERE r.id = %s",
        (report_id,)
    )

    if not report_info_raw:
        await query.answer("⚠️ Ошибка: отчет не найден. Возможно, он был удален.", show_alert=True)
        return

    (report_discipline, chat_id, message_id, report_date_db, foreman_name, 
     corpus_name, work_type_name, people_count, volume, notes) = report_info_raw[0]

    if not chat_id or not message_id:
        await query.answer("⚠️ Ошибка: для этого отчета не найдена привязка к группе или ID сообщения.", show_alert=True)
        return

    is_authorized = False
    if user_role.get('isKiok') and user_role.get('discipline') == report_discipline:
        is_authorized = True
    elif user_role.get('isAdmin'):
        is_authorized = True

    if not is_authorized:
        await query.answer("⛔️ У вас нет прав для согласования этого отчета.", show_alert=True)
        return
        
    await query.answer("✅ Решение принято. Обновляю статус...")

    approver_name_query = """
        SELECT first_name, last_name FROM kiok WHERE user_id = %s
        UNION ALL
        SELECT first_name, last_name FROM pto WHERE user_id = %s
        UNION ALL
        SELECT first_name, last_name FROM managers WHERE user_id = %s
        UNION ALL
        SELECT first_name, last_name FROM admins WHERE user_id = %s
        LIMIT 1;
    """
    params = (user_id, user_id, user_id, user_id)
    user_data = db_query(approver_name_query, params)

    if user_data and user_data[0]:
        first_name, last_name = user_data[0]
        approver_name = f"{first_name or ''} {last_name or ''}".strip()
    else:
        approver_name = query.from_user.full_name

    new_status = 1 if action == 'approve' else -1
    db_query(
        "UPDATE reports SET kiok_approved = %s, kiok_approver_id = %s, kiok_approval_timestamp = %s WHERE id = %s",
        (new_status, user_id, datetime.now(), report_id)
    )

    report_date_display = report_date_db.strftime("%d.%m.%Y")

    status_text = f"✅ Согласовано: {approver_name}" if action == 'approve' else f"❌ Отклонено: {approver_name}"
    
    unit_of_measure_raw = db_query("SELECT unit_of_measure FROM work_types WHERE name = %s", (work_type_name,))
    unit_of_measure = unit_of_measure_raw[0][0] if unit_of_measure_raw and unit_of_measure_raw[0][0] else ""

    report_lines = [
        f"📄 *Отчет от бригадира: {foreman_name}* (ID: {report_id})\n",
        f"▪️ *Корпус:* {corpus_name}",
        f"▪️ *Дисциплина:* {report_discipline}", 
        f"▪️ *Вид работ:* {work_type_name}",
        f"▪️ *Дата:* {report_date_display}",
        f"▪️ *Кол-во человек:* {people_count}",
        f"▪️ *Выполненный объем:* {volume} {unit_of_measure}"
    ]
    if notes:
        report_lines.append(f"▪️ *Примечание:* {notes}")
    
    report_lines.append(f"\n*Статус:* {status_text}")
    final_text = "\n".join(report_lines)

    logger.info(f"Попытка отредактировать сообщение: chat_id={chat_id}, message_id={message_id}")
    
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=final_text,
            parse_mode="Markdown",
            reply_markup=None 
        )
        logger.info("Сообщение успешно отредактировано!")
    except Exception as e:
        logger.error(f"НЕ УДАЛОСЬ ОТРЕДАКТИРОВАТЬ СООБЩЕНИЕ! Ошибка: {e}")

# --- ЛОКАЛИЗАЦИЯ ЯЗЫКОВ ---

def get_user_language(user_id: str) -> str:
    """Получает код языка пользователя с ОТЛАДКОЙ."""
    user_id_str = str(user_id)
    logger.info(f"[DEBUG] --- Начинаю поиск языка для {user_id_str} ---")
    tables = ['admins', 'managers', 'brigades', 'pto', 'kiok']
    for table in tables:
        table_exists = db_query(f"SELECT to_regclass('public.{table}')")
        if table_exists and table_exists[0][0]:
            col_check = db_query(f"SELECT 1 FROM information_schema.columns WHERE table_name='{table}' AND column_name='language_code' LIMIT 1")
            if col_check:
                logger.info(f"[DEBUG] Проверяю таблицу {table} для {user_id_str}...")
                lang_code_raw = db_query(f"SELECT language_code FROM {table} WHERE user_id = %s", (user_id_str,))

                if lang_code_raw and lang_code_raw[0] and lang_code_raw[0][0]:
                    found_lang = lang_code_raw[0][0]
                    logger.info(f"[DEBUG] НАЙДЕН ЯЗЫК! В таблице {table} для {user_id_str} стоит '{found_lang}'. Возвращаю его.")
                    return found_lang
    
    logger.info(f"[DEBUG] Язык не найден ни в одной таблице. Возвращаю 'ru' по умолчанию для {user_id_str}.")
    return 'ru'

def update_user_language(user_id: str, lang_code: str):
    """Обновляет язык пользователя с ОТЛАДКОЙ."""
    user_id_str = str(user_id)
    tables = ['admins', 'managers', 'brigades', 'pto', 'kiok']
    logger.info(f"[DEBUG] === Начинаю обновление языка для {user_id_str} на '{lang_code}' ===")
    for table in tables:

        table_exists = db_query(f"SELECT to_regclass('public.{table}')")
        if table_exists and table_exists[0][0]:
            col_check = db_query(f"SELECT 1 FROM information_schema.columns WHERE table_name='{table}' AND column_name='language_code' LIMIT 1")
            if col_check:

                db_query(f"UPDATE {table} SET language_code = %s WHERE user_id = %s", (lang_code, user_id_str))
                logger.info(f"[DEBUG] Выполнена попытка UPDATE для таблицы {table}.")
    logger.info(f"[DEBUG] === Завершение обновления языка для {user_id_str} ===")

async def select_language_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает инлайн-кнопки для выбора языка (работает и от команды, и от кнопки)."""
    lang = get_user_language(str(update.effective_user.id))
    
    keyboard = [
        [InlineKeyboardButton("English 🇬🇧", callback_data="set_lang_en")],
        [InlineKeyboardButton("Русский 🇷🇺", callback_data="set_lang_ru")],
        [InlineKeyboardButton("O'zbekcha 🇺🇿", callback_data="set_lang_uz")],
    ]
    # Если это не первое приветствие, добавим кнопку "Назад"
    if update.callback_query:
        keyboard.append([InlineKeyboardButton(get_text('back_button', lang), callback_data="go_back_to_main_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    text = get_text('language_prompt', lang)

    # Если функция вызвана нажатием на кнопку, редактируем сообщение
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    # Если вызвана командой /language, отправляем новое
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def set_language_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатие на кнопку выбора языка."""
    query = update.callback_query
    await query.answer()
    
    lang_code = query.data.split('_')[-1]
    user_id = str(query.from_user.id)

    update_user_language(user_id, lang_code)

    await query.edit_message_text(get_text('language_changed', lang_code))
    await show_main_menu_logic(context, user_id, query.message.chat_id)

# --- ГЛАВНАЯ ФУНКЦИЯ ---
def main() -> None:
    """Главная функция запуска бота с корректной интеграцией планировщика."""
    #init_db() # Создания БД с нуля.
    ensure_dirs_exist()
    
    builder = Application.builder().token(TOKEN)
    builder.post_init(post_init)
    builder.post_stop(post_stop)
    application = builder.build()

    restore_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(prompt_for_restore_file, pattern="^db_backup_upload_prompt$")],
        states={
            AWAITING_RESTORE_FILE: [MessageHandler(filters.Document.MimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"), handle_db_restore_file)]
        },
        fallbacks=[CommandHandler('cancel', cancel_restore),
                   CommandHandler('start', start_over)
                   ],
        per_user=True
    )
    
    hr_date_conv_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(get_hr_date, pattern="^hr_date_select_")],
    states={
        GETTING_HR_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_hr_date)]
    },
    fallbacks=[CommandHandler('cancel', show_hr_menu), CommandHandler('start', start_over)],
    per_user=True, allow_reentry=True
)
    
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_auth, pattern="^start_auth$")],
        states={
            SELECTING_ROLE: [CallbackQueryHandler(select_role, pattern="^auth_")],
            GETTING_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_name)],
            GETTING_CONTACT: [MessageHandler(filters.CONTACT, get_contact)],
            SELECTING_MANAGER_LEVEL: [CallbackQueryHandler(handle_manager_level, pattern="^level_")],
            SELECTING_DISCIPLINE: [CallbackQueryHandler(handle_discipline, pattern="^disc_")],
        },
        fallbacks=[CallbackQueryHandler(cancel_auth, pattern="^cancel_auth$"),
                   CommandHandler('start', start_over)
                   ],
        per_user=True, per_chat=True, allow_reentry=True
    )
 
    roster_conv_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(start_roster_submission, pattern="^submit_roster$")],
    states={
        AWAITING_ROLES_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_role_counts)],
        CONFIRM_ROSTER: [CallbackQueryHandler(save_roster, pattern="^confirm_roster$")],
        CONFIRM_DANGEROUS_ROSTER_SAVE: [CallbackQueryHandler(execute_dangerous_roster_save, pattern="^force_save_roster$")],
    },
    fallbacks=[
    CallbackQueryHandler(cancel_roster_submission, pattern="^cancel_roster$"),
    CommandHandler('start', start_over)
],
    per_user=True
)

    report_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_report, pattern="^new_report$")],
        states={

           OWNER_SELECTING_DISCIPLINE: [
            CallbackQueryHandler(owner_select_discipline_and_ask_corpus, pattern="^owner_select_disc_")
        ],
           GETTING_CORPUS: [
                CallbackQueryHandler(get_corpus_and_ask_work_type, pattern="^report_corp_"),
                CallbackQueryHandler(paginate_corps, pattern="^paginate_corps_"),
            ],
            GETTING_WORK_TYPE: [
                CallbackQueryHandler(get_work_type_and_ask_count, pattern="^report_work_"),
                CallbackQueryHandler(paginate_work_types, pattern="^paginate_work_types_"),
            ],
            GETTING_PEOPLE_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_people_count_and_ask_volume)],
            GETTING_VOLUME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_volume_and_ask_date)],
            GETTING_DATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_date_and_ask_notes),
                CallbackQueryHandler(get_date_and_ask_notes, pattern="^set_date_")
            ],
            GETTING_NOTES: [
                CallbackQueryHandler(prompt_for_note, pattern="^add_note$"),
                CallbackQueryHandler(skip_note_and_confirm, pattern="^skip_note$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_note_and_confirm),
            ],
            CONFIRM_REPORT: [CallbackQueryHandler(submit_report, pattern="^submit_report$")],
        },
        fallbacks=[
             CallbackQueryHandler(cancel_report, pattern="^cancel_report$"),
             CallbackQueryHandler(go_back_in_report_creation, pattern="^back_to_"),
             CommandHandler('start', start_over)
],
        per_user=True, per_chat=True, allow_reentry=True
    )

    level_change_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(set_level, pattern="^set_level_")],
    states={
        AWAITING_DISCIPLINE_FOR_MANAGER: [CallbackQueryHandler(set_new_discipline_for_manager, pattern="^set_new_disc_")]
    },
    fallbacks=[CallbackQueryHandler(cancel_admin_action, pattern="^cancel_admin_action$")],
    per_user=True
)
    admin_management_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(admin_report_menu, pattern="^admin_report_menu_start$")
        ],
        states={
            SELECT_DISC_FOR_EDIT: [
                CallbackQueryHandler(admin_select_discipline, pattern="^admin_disc_")
            ],
            SELECT_BRIGADE_FOR_EDIT: [
                CallbackQueryHandler(admin_select_brigade, pattern="^admin_brig_"),
                CallbackQueryHandler(admin_select_discipline, pattern="^admin_disc_")
            ],
            SELECT_REPORT_FOR_EDIT: [
                CallbackQueryHandler(admin_confirm_delete, pattern="^admin_delete_"),
                CallbackQueryHandler(start_report_edit, pattern="^admin_edit_"),
                CallbackQueryHandler(admin_show_reports_by_button, pattern="^admin_show_date_"),
                CallbackQueryHandler(admin_prompt_for_date, pattern="^admin_pick_date$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_process_date_input),
                CallbackQueryHandler(admin_select_discipline, pattern="^admin_disc_"),
                CallbackQueryHandler(admin_select_brigade, pattern="^admin_brig_"),
            ],
            CONFIRM_DELETE: [
                CallbackQueryHandler(admin_execute_delete, pattern="^admin_delete_confirm_yes$"),
                CallbackQueryHandler(admin_select_brigade, pattern="^admin_brig_")
            ],
            SELECT_FIELD_TO_EDIT: [
                CallbackQueryHandler(prompt_for_new_value, pattern="^edit_field_"),
                CallbackQueryHandler(save_edited_report, pattern="^edit_save$"),
                CallbackQueryHandler(cancel_edit, pattern="^edit_cancel$")
            ],
            AWAITING_NEW_VALUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_new_value),
                CallbackQueryHandler(process_new_value, pattern="^set_new_value_")
            ]
        },
        fallbacks=[
            CallbackQueryHandler(cancel_admin_operation, pattern="^cancel_admin_op$"),
            CommandHandler('start', start_over)
        ],
        per_user=True,
        allow_reentry=True
    )

    application.add_handler(admin_management_conv)

    overview_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(show_overview_dashboard_menu, pattern="^report_overview$")],
        states={
            SELECTING_OVERVIEW_ACTION: [

                CallbackQueryHandler(show_overview_dashboard_menu, pattern="^report_overview_date_"),
                CallbackQueryHandler(prompt_for_overview_date, pattern="^report_overview_pick_date$"),
                CallbackQueryHandler(generate_overview_chart, pattern="^gen_overview_chart_"),
            ],
            AWAITING_OVERVIEW_DATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_overview_date),
                CallbackQueryHandler(show_overview_dashboard_menu, pattern="^report_overview_date_"),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(report_menu, pattern="^report_menu_all$"),
            CommandHandler('start', start_over)
        ],
        per_user=True, allow_reentry=True
    )

    application.add_handler(overview_conv_handler)
    application.add_handler(level_change_handler)
    application.add_handler(hr_date_conv_handler)
    application.add_handler(restore_conv_handler)
    application.add_handler(conv_handler)
    application.add_handler(report_conv_handler)
    application.add_handler(roster_conv_handler)
    
      
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(handle_approval, pattern="^(approve_|reject_)"))
    application.add_handler(CallbackQueryHandler(handle_kiok_decision, pattern="^kiok_"))
    application.add_handler(CallbackQueryHandler(show_profile, pattern="^show_profile$"))
    application.add_handler(CallbackQueryHandler(report_menu, pattern="^report_menu_"))
    application.add_handler(CallbackQueryHandler(report_overview_chart_prompt, pattern="^report_overview_chart_prompt$"))
    application.add_handler(CallbackQueryHandler(report_menu, pattern="^report_menu_all$"))
    application.add_handler(CallbackQueryHandler(manage_menu, pattern="^manage_menu$"))
    application.add_handler(CallbackQueryHandler(generate_overview_chart, pattern="^gen_overview_chart_"))
    #application.add_handler(CallbackQueryHandler(show_overview_dashboard_menu, pattern="^report_overview$"))
    
    
    #application.add_handler(CallbackQueryHandler(show_problem_brigades_menu, pattern="^report_underperforming$"))
    application.add_handler(CallbackQueryHandler(handle_problem_brigades_button, pattern="^handle_problem_brigades_button$"))
    application.add_handler(CallbackQueryHandler(generate_problem_brigades_report, pattern="^gen_problem_report_"))
    application.add_handler(CallbackQueryHandler(show_foreman_performance, pattern="^foreman_performance$"))
    application.add_handler(CallbackQueryHandler(show_historical_report_menu, pattern="^report_historical$"))
    application.add_handler(CallbackQueryHandler(generate_discipline_dashboard, pattern="^gen_hist_report_"))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_directories_excel))
    application.add_handler(CallbackQueryHandler(export_reports_to_excel, pattern="^get_excel_report$"))
    application.add_handler(CallbackQueryHandler(manage_menu, pattern="^manage_menu$"))
    application.add_handler(CallbackQueryHandler(manage_users_menu, pattern="^manage_users$"))
    application.add_handler(CallbackQueryHandler(manage_directories_menu, pattern="^manage_directories$"))
    application.add_handler(CallbackQueryHandler(get_directories_template, pattern="^get_directories_template_button$"))
    application.add_handler(CallbackQueryHandler(manage_db_menu, pattern="^manage_db$"))
    application.add_handler(CallbackQueryHandler(download_db_backup, pattern="^db_backup_download$"))
    application.add_handler(CallbackQueryHandler(export_all_users_to_excel, pattern="^db_export_all_users$"))
    application.add_handler(CommandHandler("link_topic", link_topic))
    application.add_handler(CallbackQueryHandler(list_users, pattern="^list_users_"))
    application.add_handler(CallbackQueryHandler(delete_user, pattern="^delete_user_"))
    application.add_handler(CallbackQueryHandler(show_user_edit_menu, pattern="^edit_user_"))
    application.add_handler(CallbackQueryHandler(show_discipline_change_menu, pattern="^change_discipline_"))
    application.add_handler(CallbackQueryHandler(set_discipline, pattern="^set_discipline_"))
    application.add_handler(CallbackQueryHandler(show_level_change_menu, pattern="^change_level_"))
    application.add_handler(CommandHandler("add_admin", add_admin))
    application.add_handler(CallbackQueryHandler(back_to_main_menu, pattern="^go_back_to_main_menu$"))
    application.add_handler(CallbackQueryHandler(back_to_main_menu, pattern="^main_menu_from_profile$"))
    application.add_handler(CallbackQueryHandler(list_reports_for_deletion, pattern="^delete_report_list_"))
    application.add_handler(CallbackQueryHandler(confirm_delete_report, pattern="^confirm_delete_"))
    application.add_handler(CallbackQueryHandler(execute_delete_report, pattern="^execute_delete_"))
    application.add_handler(CallbackQueryHandler(confirm_reset_roster, pattern="^reset_roster_"))
    application.add_handler(CallbackQueryHandler(execute_reset_roster, pattern="^execute_reset_roster_"))
    application.add_handler(CommandHandler("language", select_language_menu))
    application.add_handler(CallbackQueryHandler(set_language_callback, pattern="^set_lang_"))
    application.add_handler(CallbackQueryHandler(show_hr_menu, pattern="^hr_menu$"))
    application.add_handler(CallbackQueryHandler(show_paginated_brigade_report, pattern="^hr_report_"))
    application.add_handler(CallbackQueryHandler(select_language_menu, pattern="^select_language$"))
    application.add_handler(CallbackQueryHandler(show_problem_brigades_by_date, pattern="^problem_brigades_by_date_"))

    logger.info("Бот запущен...")
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()