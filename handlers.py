import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes, ConversationHandler, CommandHandler, MessageHandler, filters, CallbackQueryHandler
import telegram.error
import config
import database as db
import questions
import utils
from datetime import datetime, timedelta
import pytz
import sqlite3

# Состояния диалога
(CHOICE,
 ENGAGEMENT_Q1, ENGAGEMENT_Q2, ENGAGEMENT_Q3,
 PRESENTATION_1, PRESENTATION_2, PRESENTATION_3, PRESENTATION_4, PRESENTATION_5,
 AFTER_PRESENTATION,
 TEST,
 FEEDBACK,
 VIDEO_OFFER,
 WAITING_VIDEO,
 MEETING_FORMAT,
 MEETING_TIME,
 CUSTOM_TIME,
 ASK_CITY,
 PRODUCTS,
 MEETING_CONFIRMED) = range(20)

# -----------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -----------------------------------------------------------------------------
def get_inviter_name_safe(user_id):
    inviter_id = db.get_invited_by(user_id)
    return db.get_inviter_name(inviter_id) or "твой друг"

def get_inviter_mention(user_id):
    inviter_id = db.get_invited_by(user_id)
    if inviter_id:
        name = db.get_inviter_name(inviter_id) or "друг"
        username = db.get_username(inviter_id)
        return f"@{username}" if username else name
    return "я"

def slot_to_timestamp(slot, reference=None):
    tz = pytz.timezone('Europe/Moscow')
    now = datetime.now(tz)
    if slot == "tomorrow_10":
        dt = now + timedelta(days=1)
        dt = dt.replace(hour=10, minute=0, second=0, microsecond=0)
    elif slot == "tomorrow_12":
        dt = now + timedelta(days=1)
        dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
    elif slot == "tomorrow_15":
        dt = now + timedelta(days=1)
        dt = dt.replace(hour=15, minute=0, second=0, microsecond=0)
    elif slot == "dayafter_10":
        dt = now + timedelta(days=2)
        dt = dt.replace(hour=10, minute=0, second=0, microsecond=0)
    elif slot == "dayafter_12":
        dt = now + timedelta(days=2)
        dt = dt.replace(hour=12, minute=0, second=0, microsecond=0)
    elif slot == "dayafter_15":
        dt = now + timedelta(days=2)
        dt = dt.replace(hour=15, minute=0, second=0, microsecond=0)
    else:
        return None
    return int(dt.timestamp())

# -----------------------------------------------------------------------------
# ФУНКЦИЯ ДЛЯ ПОДБОРА ИСТОРИИ ПО СЛАБОМУ МЕСТУ (НОВАЯ)
# -----------------------------------------------------------------------------
def get_story_for_weak(weak):
    """Возвращает универсальную историю по слабому месту (без привязки к полу)."""
    stories = {
        'учёт и контроль': (
            "До входа в проект один из моих партнёров тоже не вёл учёт и деньги утекали незаметно. "
            "А когда начал записывать траты в приложении, через месяц удивился: "
            "«Я и не думал, что столько уходит на кофе!» Теперь у него всегда есть подушка безопасности."
        ),
        'сбережения': (
            "До входа в проект один из моих партнёров жил от зарплаты до зарплаты, любые непредвиденные расходы — стресс. "
            "А когда оформил накопительный счёт и начал откладывать по 1000 рублей в неделю, "
            "через год накопил на небольшой отпуск. Говорит: «Лучше маленькими шагами, чем никак»."
        ),
        'кредиты': (
            "У одного из моих партнёров до входа в проект была кредитная карта с высокими процентами. "
            "Перевёл долг в Альфа-Банк по ставке ниже и закрыл кредит досрочно. "
            "А сейчас с выгодой для себя пользуется кредиткой и делает покупки на WB и OZON с кэшбеком 15% "
            "+ имеет проценты с кредитных денег на накопительном счёте."
        ),
        'инвестиции': (
            "Многие боятся инвестиций: «Вдруг всё потеряю?» Но можно начать с малого — "
            "воспользоваться предложением банка, купить акции и получить проценты +, "
            "10 тыс. выплаты через 6 месяцев. За это время понимаешь, "
            "что деньги могут работать сами."
        ),
        'планирование': (
            "До входа в проект один из моих партнёров никогда не ставил финансовых целей. Всё было «как-нибудь само». "
            "А когда записал на бумаге: «Хочу через год купить машину», — начал откладывать. "
            "Через полтора года купил! Говорит: «Цель работает как магнит»."
        ),
    }
    return stories.get(weak, "Многие сомневались, но попробовали — и через месяц получили первые деньги. Главное — начать!")

# -----------------------------------------------------------------------------
# БЕЗОПАСНАЯ ОТПРАВКА СООБЩЕНИЙ
# -----------------------------------------------------------------------------
async def safe_send_message(update_or_context, chat_id, text, **kwargs):
    try:
        if hasattr(update_or_context, 'bot'):
            bot = update_or_context.bot
        else:
            bot = update_or_context
        await bot.send_message(chat_id=chat_id, text=text, **kwargs)
        return True
    except telegram.error.Forbidden:
        db.set_user_blocked(chat_id, 1)
        logging.warning(f"Пользователь {chat_id} заблокировал бота, помечен в БД.")
        return False
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения {chat_id}: {e}")
        return False

# -----------------------------------------------------------------------------
# СТАРТ
# -----------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = context.args
    invited_by = None
    if args and args[0].isdigit():
        invited_by = int(args[0])
        if invited_by == user.id:
            invited_by = None

    db.add_user(user.id, user.username, user.first_name, invited_by)
    db.update_last_active(user.id)

    inviter_name = get_inviter_name_safe(user.id)

    text = (
        f"Привет, {user.first_name}! 👋\n\n"
        f"{inviter_name} просит меня отправить тебе тест. Но я сразу скажу: тут есть два варианта, выбирай тот, который ближе:\n\n"
        f"1️⃣ **Пройди тест** — 10 вопросов, 5 минут. Узнаешь, где у тебя деньги “утекают”, и получишь пару простых советов. А заодно поможешь с обратной связью.\n\n"
        f"2️⃣ **Посмотри, как тут зарабатывают** — если тема дохода интереснее, чем тесты. Расскажу, сколько можно получать и с чего начать.\n\n"
        f"Жми кнопку ниже, чтобы продолжить 👇\n\n"
        f"⚠️ Если Telegram работает медленно или кнопки не нажимаются, установите приложение «Телега» (https://play.google.com/store/apps/details?id=ru.dahl.messenger) или включите VPN (TiPTop: https://play.google.com/store/apps/details?id=com.free.tiptop.vpn.proxy). После этого бот будет отвечать быстро."
    )
    await update.message.reply_text(
        text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Старт", callback_data="show_choice")]]),
        disable_web_page_preview=True  # Отключает предпросмотр ссылок
    )
    return CHOICE

async def show_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("✅ Пройти тест", callback_data="start_test")],
        [InlineKeyboardButton("💰 Хочу узнать про доход", callback_data="start_income")]
    ]
    await query.message.reply_text("Что выберешь? 👇", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOICE

async def choice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    logging.info(f"choice_handler ВЫЗВАН с callback_data={query.data}")
    user = query.from_user
    try:
        await query.answer()
    except telegram.error.BadRequest as e:
        logging.warning(f"Не удалось ответить на callback (возможно, устарел): {e}")
    except Exception as e:
        logging.error(f"Ошибка при answer_callback_query: {e}")

    if query.data == "start_test":
        logging.info("choice_handler: запускаем тест")
        db.set_chosen_path(user.id, 'test')
        context.user_data['answers'] = []
        context.user_data['q_index'] = 0
        await send_test_question(query, context)
        return TEST
    else:  # start_income
        logging.info("choice_handler: запускаем путь дохода")
        db.set_chosen_path(user.id, 'income')
        await send_engagement_q1(query, context)
        return ENGAGEMENT_Q1

# -----------------------------------------------------------------------------
# ВОВЛЕКАЮЩИЕ ВОПРОСЫ (путь дохода)
# -----------------------------------------------------------------------------
async def send_engagement_q1(update_or_query, context):
    text = (
        "Окей, давай прикинем, что тебе подойдёт. Ответь на пару вопросов:\n\n"
        "1️⃣ **На какой доход через 3 месяца рассчитываешь?** (примерно)"
    )
    keyboard = [
        [InlineKeyboardButton("20+ тыс (на старте)", callback_data="eng_1")],
        [InlineKeyboardButton("30–45 тыс (1-2 часа в день)", callback_data="eng_2")],
        [InlineKeyboardButton("50–100 тыс (3-4 часа)", callback_data="eng_3")],
        [InlineKeyboardButton("больше 100 тыс (тогда нам точно по пути)", callback_data="eng_4")]
    ]
    await update_or_query.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def engagement_q1_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"engagement_q1_handler ВЫЗВАН с callback_data={update.callback_query.data}")
    query = update.callback_query
    await query.answer()
    context.user_data['income_goal'] = query.data
    text = "2️⃣ **Сколько времени в день можешь уделять?**"
    keyboard = [
        [InlineKeyboardButton("1 час", callback_data="time_1")],
        [InlineKeyboardButton("2–3 часа", callback_data="time_2")],
        [InlineKeyboardButton("4+ часов", callback_data="time_3")]
    ]
    await query.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    return ENGAGEMENT_Q2

async def engagement_q2_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"engagement_q2_handler ВЫЗВАН с callback_data={update.callback_query.data}")
    query = update.callback_query
    await query.answer()
    context.user_data['time'] = query.data
    text = (
        "3️⃣ **Кто из знакомых тоже может заинтересоваться такой темой?** (просто подумай, никому не скажу 😉)"
    )
    keyboard = [
        [InlineKeyboardButton("Есть несколько", callback_data="circle_yes")],
        [InlineKeyboardButton("Пара человек", callback_data="circle_few")],
        [InlineKeyboardButton("Пока никого", callback_data="circle_no")]
    ]
    await query.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    return ENGAGEMENT_Q3

async def engagement_q3_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"engagement_q3_handler ВЫЗВАН с callback_data={update.callback_query.data}")
    query = update.callback_query
    await query.answer()
    context.user_data['circle'] = query.data
    await send_presentation_part1(query, context)
    return PRESENTATION_1

# -----------------------------------------------------------------------------
# ПОЛНАЯ ПРЕЗЕНТАЦИЯ (путь дохода) – 3 части
# -----------------------------------------------------------------------------
async def send_presentation_part1(update_or_query, context):
    text = (
        "**Часть 1**\n\n"
        "Проект называется **«Свой в Альфе»** — это официальный проект от Альфа-Банка (топ-3 в России).\n\n"
        "Суть простая: ты советуешь людям продукты банка (карты, счета, вклады и т.д.) — банк платит. Без вложений, без продаж, удалённо. Честно и прозрачно.\n\n"
        "Но это не только доход! Для активных партнёров работает программа **«Всё своё для своих»**:\n"
        "• 📞 **Бесплатная мобильная связь** — звонки и интернет за счёт банка.\n"
        "• 🏥 **ДМС до 30 млн рублей** — полное медицинское обслуживание, стоматология, анализы.\n"
        "• 💰 **+10% к пенсии каждый год** — доплата от твоего заработка в проекте.\n"
        "• 🎉 **Альфа-Клуб** — мероприятия по всей России, встречи с топ-лидерами.\n"
        "• 🏆 **Призы и награды** — гаджеты, путешествия, автомобили.\n"
        "• 🤝 **Сообщество единомышленников** — поддержка, обмен опытом, рост.\n\n"
        "А ещё у нас есть **бизнес-карта** с 10% кэшбэка на популярные категории и бонусы до 6000 рублей за первые шаги. "
        "Каждую пятницу банк дарит по 3000 рублей подарком! 🔥\n\n"
        "Самое крутое — у нас есть **умный бот**🤖, который автоматически ведёт новичков: показывает презентацию, собирает обратную связь, назначает встречи🤝. "
        "А в ближайшее время мы добавим **ИИ-агента**✨, который будет отвечать на вопросы 24/7 и помогать на каждом шагу. Это позволит тебе заниматься только самым важным — общением с заинтересованными людьми.\n\n"
        "Идём дальше? 👇"
    )
    await update_or_query.message.reply_text(
        text, parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Далее", callback_data="pres_next")]])
    )
    context.user_data['pres_part'] = 1
    return PRESENTATION_1

async def send_presentation_part2(update_or_query, context):
    text = (
        "**Часть 2**\n\n"
        "Теперь про деньги. Реальные цифры:\n\n"
        "• **Старт:** 20+ тыс/мес (сам работаешь)\n"
        "• **Через 3 месяца:** 30–45 тыс/мес (небольшая команда)\n"
        "• **Через полгода:** 80–150 тыс/мес (активная команда)\n"
        "• Некоторые вообще выходят на 300–500 тыс.\n\n"
        "Вот, например, Анна из нашей команды: она вообще не верила, что можно зарабатывать без продаж. "
        "Когда я ей впервые показал эти цифры, она сказала: «Это сказки». А через полгода у неё было 120 тысяч, и она уволилась с работы. "
        "Я тогда сам удивился — оказывается, система работает даже для тех, кто сомневается.\n\n"
        "Ну как, похоже на то, что ищешь?"
    )
    await update_or_query.message.reply_text(
        text, parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Далее", callback_data="pres_next")]])
    )
    context.user_data['pres_part'] = 2
    return PRESENTATION_2

async def send_presentation_part3(update_or_query, context):
    user = update_or_query.from_user
    inviter_name = get_inviter_name_safe(user.id)
    weak = context.user_data.get('weak')
    
    # Словарь для правильного падежа (с чем?)
    weak_prepositional = {
        'учёт и контроль': 'учётом и контролем',
        'сбережения': 'сбережениями',
        'кредиты': 'кредитами',
        'инвестиции': 'инвестициями',
        'планирование': 'планированием',
    }
    weak_form = weak_prepositional.get(weak, weak)  # если нет в словаре, оставляем как есть
    
    if weak:
        experience = f"У меня ушла проблема с **{weak_form}**. За первый месяц 20 000 ₽ + 6 000 бонусов от банка."
    else:
        experience = f"За первый месяц я получил 20 000 ₽ + 6 000 бонусов от банка."

    text = (
        "**Часть 3**\n\n"
        f"Что делать прямо сейчас:\n\n"
        f"{inviter_name} предложит тебе варианты встречи. Согласуйте дату и время.\n\n"
        f"{experience} У ребят из команды через полгода – 80–150 тысяч, а некоторые до 300–500 тысяч доходят. И это не потолок – через команду можно выйти на пассивный доход.\n\n"
        "А пока — небольшой бонус: при оформлении дебетовой карты сейчас действует **весенний кэшбэк**:\n"
        "• 10% на топливо, ремонт дома, супермаркеты, аптеки\n"
        "• 15% на маркетплейсы (Ozon, Wildberries и др.)\n"
        "Это реальная экономия на повседневных тратах. Карта бесплатная, курьер привезёт куда удобно.\n\n"
        "Звучит реально? 👇"
    )
    await update_or_query.message.reply_text(
        text, parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Далее", callback_data="pres_next")]])
    )
    context.user_data['pres_part'] = 3
    return PRESENTATION_3

async def send_presentation_part4(update_or_query, context):
    logging.error("send_presentation_part4 вызвана, но не должна")
    return await after_presentation(update_or_query, context)

async def send_presentation_part5(update_or_query, context):
    logging.error("send_presentation_part5 вызвана, но не должна")
    return await after_presentation(update_or_query, context)

async def presentation_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    part = context.user_data.get('pres_part', 1)
    if part == 1:
        return await send_presentation_part2(query, context)
    elif part == 2:
        return await send_presentation_part3(query, context)
    elif part == 3:
        return await after_presentation(query, context)
    else:
        return await after_presentation(query, context)

async def after_presentation(update_or_query, context):
    user = update_or_query.from_user
    inviter_name = get_inviter_name_safe(user.id)
    text = (
        f"Теперь ты в курсе, как это работает. Если хочешь попробовать, {inviter_name} поможет с первыми шагами. "
        f"Я передам, что ты хочешь начать. Договорились?"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, хочу", callback_data="partnership_yes")],
        [InlineKeyboardButton("❌ Нет, не сейчас", callback_data="partnership_no")]
    ])
    await update_or_query.message.reply_text(text, reply_markup=keyboard)
    return AFTER_PRESENTATION

async def partnership_yes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info("🔥 partnership_yes ВЫЗВАНА")
    query = update.callback_query
    await query.answer()
    user = query.from_user
    db.set_user_ready(user.id)
    return await offer_meeting(query, context, from_test=False)

async def partnership_no(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info("🔥 partnership_no ВЫЗВАНА")
    query = update.callback_query
    await query.answer()
    await send_products_and_recommendations(query, context)
    return PRODUCTS

# -----------------------------------------------------------------------------
# ТЕСТ
# -----------------------------------------------------------------------------
async def send_test_question(update_or_query, context):
    idx = context.user_data['q_index']
    logging.info(f"send_test_question: idx={idx}")
    if idx >= len(questions.questions):
        return await finish_test(update_or_query, context)
    q = questions.questions[idx]
    text = f"*Вопрос {idx+1}/{len(questions.questions)}*\n{q[0]}"
    keyboard = []
    for i, opt in enumerate(q[1]):
        keyboard.append([InlineKeyboardButton(opt, callback_data=f"test_ans_{i}")])
    await update_or_query.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    return TEST

async def test_answer_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    logging.info(f"test_answer_handler: получен callback с data={query.data}")
    await query.answer()
    ans = int(query.data.split('_')[-1])
    context.user_data['answers'].append(ans)
    context.user_data['q_index'] += 1
    return await send_test_question(query, context)

async def finish_test(update_or_query, context):
    try:
        answers = context.user_data['answers']
        logging.info(f"finish_test: получено {len(answers)} ответов")
        total, cat_scores = utils.calculate_score(answers)
        logging.info(f"finish_test: total={total}, cat_scores={cat_scores}")
        user_id = update_or_query.from_user.id
        result_text, weak = utils.interpret_score(total, cat_scores)
        logging.info(f"finish_test: weak={weak}")
        context.user_data['weak'] = weak
        context.user_data['cat_scores'] = cat_scores
        context.user_data['test_total'] = total

        weak_messages = {
            'учёт и контроль': (
                "Ты не всегда контролируешь свои расходы, из-за чего деньги утекают незаметно. "
                "Это частая проблема, но её легко исправить."
            ),
            'сбережения': (
                "Ты почти не откладываешь, и любая неожиданная трата выбивает из колеи. "
                "Подушка безопасности — это база, без неё чувствуешь себя неуверенно."
            ),
            'кредиты': (
                "Кредиты есть, и иногда они доставляют дискомфорт. "
                "Возможно, условия не самые выгодные, и их можно улучшить."
            ),
            'инвестиции': (
                "Деньги могли бы работать на тебя, но пока ты не в теме инвестиций. "
                "Это нормально, многие начинают с нуля."
            ),
            'планирование': (
                "У тебя нет чётких финансовых целей или плана на будущее. "
                "Без цели сложно двигаться, но её легко поставить."
            ),
        }

        weak_text = weak_messages.get(weak, "Это мешает тебе чувствовать себя уверенно. Но всё поправимо.")

        cat_names = {
            'control': 'учёт и контроль',
            'savings': 'сбережения',
            'debts': 'кредиты',
            'invest': 'инвестиции',
            'planning': 'планирование',
            'motivation': 'мотивация'
        }

        category_comments = []
        for cat, score in cat_scores.items():
            if cat == 'motivation':
                continue
            max_score_map = {
                'control': 2 * 3,
                'savings': 3 * 3,
                'debts': 2 * 3,
                'invest': 1 * 3,
                'planning': 2 * 3,
            }
            max_score = max_score_map.get(cat, 3)
            if score <= max_score * 0.33:
                level = "низкий"
                comment = f"Вам стоит серьёзно заняться {cat_names[cat]}."
            elif score <= max_score * 0.66:
                level = "средний"
                comment = f"В этой области есть куда расти, но вы на верном пути."
            else:
                level = "хороший"
                comment = f"У вас отличные результаты в области {cat_names[cat]}."
            category_comments.append(f"– {cat_names[cat]}: {score} баллов ({level})")

        result_lines = [
            "✅ Готово! Смотри, что получилось:",
            "",
            *category_comments,
            "",
            f"Самое слабое место — **{weak}**.",
            weak_text,
            "",
            f"Опиши парой слов, что тебя больше всего беспокоит в этой ситуации. Например: «вечно не хватает до зарплаты» или «боюсь брать кредиты». Это поможет мне точнее подобрать совет 👇"
        ]

        await update_or_query.message.reply_text("\n".join(result_lines), parse_mode='Markdown')
        logging.info("finish_test: сообщение отправлено, возвращаю FEEDBACK")
        return FEEDBACK
    except Exception as e:
        logging.error(f"Ошибка в finish_test: {e}", exc_info=True)
        await update_or_query.message.reply_text("Произошла ошибка. Попробуйте ещё раз.")
        return ConversationHandler.END

async def receive_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(f"🔥 receive_feedback ВЫЗВАН с текстом: {update.message.text}")
    try:
        feedback = update.message.text
        user_id = update.effective_user.id

        test_total = context.user_data.get('test_total', 0)
        cat_scores = context.user_data.get('cat_scores', {})
        if not cat_scores:
            logging.warning("cat_scores отсутствует, используем пустой словарь")

        db.set_test_result(user_id, test_total, cat_scores, feedback)
        context.user_data['feedback'] = feedback

        weak = context.user_data.get('weak', 'финансовую подушку')
    except Exception as e:
        logging.error(f"Ошибка при сохранении фидбека: {e}", exc_info=True)
        await update.message.reply_text("Произошла ошибка. Попробуйте ещё раз.")
        return FEEDBACK

    text = (
        f"Спасибо! Ты пишешь: «{feedback}». Это знакомо многим. У меня тоже так было.\n\n"
        "В знак благодарности я отправлю тебе видео с обучения в проекте **«Свой в Альфе»**. "
        "Это выжимка самого главного (10 минут). Там реально крутые рабочие фишки:\n"
        "– Как перестать тратить на ерунду.\n"
        "– Как создать подушку безопасности с нуля.\n"
        "– Как найти дополнительный источник дохода.\n\n"
        "Отправляю? 👇"
    )
    await update.message.reply_text(
        text,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, отправь", callback_data="video_yes")],
            [InlineKeyboardButton("❌ Нет, спасибо", callback_data="video_no")]
        ])
    )
    return VIDEO_OFFER

# -----------------------------------------------------------------------------
# ВИДЕО
# -----------------------------------------------------------------------------
async def video_yes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    await query.message.reply_text(
        f"Вот ссылка: {config.VIDEO_LINK}\n\n"
        "Посмотри видео, когда будет удобно. Оно короткое — всего 10 минут.\n"
        "Как посмотришь, нажми кнопку ниже — я расскажу, что делать дальше. Если не нажмёшь, я через день напомню 😊",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Я посмотрел видео", callback_data="video_watched")]
        ])
    )
    if context.job_queue:
        job = context.job_queue.run_once(video_reminder, 24*60*60, data=user_id, name=f"remind_{user_id}")
        db.set_video_sent(user_id, str(job.id))
    else:
        logging.warning("JobQueue недоступен, напоминание не будет отправлено")
        db.set_video_sent(user_id, None)
    return WAITING_VIDEO

async def video_no(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await send_products_and_recommendations(query, context)
    return PRODUCTS

async def video_watched(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    db.confirm_video(user_id)
    if context.job_queue:
        for job in context.job_queue.jobs():
            if job.name == f"remind_{user_id}":
                job.schedule_removal()
    await offer_meeting(query, context, from_test=True)
    return MEETING_FORMAT

async def video_reminder(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    user_id = job.data
    user = db.get_user(user_id)
    if user and user[11] == 0:  # video_confirmed
        text = "Привет! Не знаю, успел ли ты посмотреть видео. Если да, нажми кнопку — продолжим. Если нет, скажи, я могу выслать ссылку ещё раз."
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Посмотрел", callback_data="video_watched")],
            [InlineKeyboardButton("🔁 Пришли ещё раз", callback_data="video_resend")],
            [InlineKeyboardButton("❌ Не интересно", callback_data="video_no")]
        ])
        await safe_send_message(context, user_id, text, reply_markup=keyboard)

async def video_resend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    await query.message.reply_text(
        f"Вот ссылка ещё раз: {config.VIDEO_LINK}\n\n"
        "Как посмотришь, нажми кнопку.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Я посмотрел видео", callback_data="video_watched")]
        ])
    )
    if context.job_queue:
        job = context.job_queue.run_once(video_reminder, 24*60*60, data=user_id, name=f"remind_{user_id}")
        db.set_video_sent(user_id, str(job.id))
    else:
        db.set_video_sent(user_id, None)
    return WAITING_VIDEO

# -----------------------------------------------------------------------------
# ПРЕДЛОЖЕНИЕ ВСТРЕЧИ (ОБНОВЛЕНО)
# -----------------------------------------------------------------------------
async def offer_meeting(update_or_query, context, from_test=False):
    user = update_or_query.from_user
    weak = context.user_data.get('weak')
    feedback = context.user_data.get('feedback')
    inviter_name = get_inviter_name_safe(user.id)

    if from_test and weak:
        story = get_story_for_weak(weak)  # больше не передаём inviter_name
        text = (
            f"Помнишь, в тесте у тебя вылезло слабое место — **{weak}** и ты пишешь: «{feedback}»?\n\n"
            f"{story}\n\n"
            f"Когда проект **«Свой в Альфе»** появился в моей жизни, эта проблема просто ушла.\n\n"
            f"За первый месяц 20 000 ₽ + 6 000 бонусов от банка. У ребят из команды через полгода – 80–150 тысяч, а некоторые до 300–500 тысяч доходят. И это не потолок – через команду можно выйти на пассивный доход.\n\n"
            f"Я могу рассказать тебе подробно, как это устроено. Выбери удобный формат и время:"
        )
    else:
        text = (
            "Ты уже знаешь, что проект реальный. Первые 20 000 за месяц — это не сказки.\n\n"
            "За первый месяц 20 000 ₽ + 6 000 бонусов от банка. У ребят из команды через полгода – 80–150 тысяч, а некоторые до 300–500 тысяч доходят. И это не потолок – через команду можно выйти на пассивный доход.\n\n"
            "Я могу рассказать тебе подробно, как это устроено. Выбери удобный формат и время:"
        )

    keyboard = [
        [InlineKeyboardButton("Онлайн-созвон", callback_data="meet_online")],
        [InlineKeyboardButton("Личная встреча (в моём городе)", callback_data="meet_offline")]
    ]
    await update_or_query.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    return MEETING_FORMAT

# -----------------------------------------------------------------------------
# ФУНКЦИИ ДЛЯ ЗАПИСИ НА ВСТРЕЧУ
# -----------------------------------------------------------------------------
async def meeting_format_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    fmt = "онлайн" if query.data == "meet_online" else "личная"
    logging.info(f"meeting_format_handler: выбран формат {fmt}")
    context.user_data['meeting_format'] = fmt
    if fmt == "личная":
        await query.message.reply_text("В каком ты городе/районе? Напиши, чтобы мы могли договориться о месте встречи.")
        return ASK_CITY
    else:
        await show_time_options(query, context)
        return MEETING_TIME

async def ask_city_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    city = update.message.text.strip()
    logging.info(f"ask_city_handler: получен город {city}")
    context.user_data['meeting_city'] = city
    await update.message.reply_text(
        "Спасибо! Если окажется, что мы в разных городах, куратор предложит встретиться онлайн. "
        "А пока выбери удобное время:"
    )
    await show_time_options(update, context)
    return MEETING_TIME

async def show_time_options(update_or_query, context):
    keyboard = [
        [InlineKeyboardButton("Завтра 10:00", callback_data="time_tomorrow_10")],
        [InlineKeyboardButton("Завтра 12:00", callback_data="time_tomorrow_12")],
        [InlineKeyboardButton("Завтра 15:00", callback_data="time_tomorrow_15")],
        [InlineKeyboardButton("Послезавтра 10:00", callback_data="time_dayafter_10")],
        [InlineKeyboardButton("Послезавтра 12:00", callback_data="time_dayafter_12")],
        [InlineKeyboardButton("Послезавтра 15:00", callback_data="time_dayafter_15")],
        [InlineKeyboardButton("Другое время", callback_data="time_other")]
    ]
    await update_or_query.message.reply_text(
        "Выбери удобное время (всё время указано **по Москве, МСК**):",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def meeting_time_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    logging.info(f"meeting_time_handler: получен callback {data}")

    if 'meeting_format' not in context.user_data:
        logging.error("meeting_format отсутствует в user_data")
        await query.message.reply_text("Ошибка, начните заново.")
        return MEETING_FORMAT

    if data == "time_other":
        await query.message.reply_text(
            "Напиши дату и время **по Москве (МСК)** в формате ДД.ММ ЧЧ.ММ, например: 27.02 19.00"
        )
        return CUSTOM_TIME
    else:
        timestamp = slot_to_timestamp(data.replace("time_", ""))
        if timestamp:
            context.user_data['meeting_timestamp'] = timestamp
            if "tomorrow" in data:
                day = "завтра"
            else:
                day = "послезавтра"
            time_map = {"10": "10:00", "12": "12:00", "15": "15:00"}
            hour = data.split('_')[-1]
            time_str = f"{day} в {time_map[hour]}"
            logging.info(f"meeting_time_handler: выбрано время {time_str}, timestamp={timestamp}")
            return await save_meeting(query, context, time_str)
        else:
            await query.message.reply_text("Ошибка, попробуйте ещё раз.")
            return MEETING_TIME

async def custom_time_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    logging.info(f"custom_time_handler: получен текст '{text}'")
    try:
        if not text:
            await update.message.reply_text("Вы ничего не ввели. Введите дату и время **по Москве (МСК)** в формате ДД.ММ ЧЧ.ММ")
            return CUSTOM_TIME

        tz = pytz.timezone('Europe/Moscow')
        now = datetime.now(tz)

        naive_dt = datetime.strptime(text, "%d.%m %H.%M").replace(year=now.year)

        if naive_dt < now.replace(tzinfo=None):
            naive_dt = naive_dt.replace(year=now.year + 1)

        dt = tz.localize(naive_dt)

        context.user_data['meeting_timestamp'] = int(dt.timestamp())
        logging.info(f"custom_time_handler: успешно распарсено -> {dt}, timestamp={int(dt.timestamp())}")
        return await save_meeting(update, context, text)

    except ValueError as ve:
        logging.error(f"custom_time_handler: ValueError при парсинге '{text}': {ve}")
        await update.message.reply_text(
            f"Ошибка формата. Введите дату и время **по Москве (МСК)** точно как в примере: 27.02 19.00 (день.месяц часы.минуты).\n"
            f"Ваш ввод: '{text}' не распознан."
        )
        return CUSTOM_TIME
    except Exception as e:
        logging.error(f"custom_time_handler: неожиданная ошибка: {e}", exc_info=True)
        await update.message.reply_text("Произошла внутренняя ошибка. Попробуйте позже.")
        return CUSTOM_TIME

async def save_meeting(update_or_query, context, time_str):
    try:
        if hasattr(update_or_query, 'from_user'):
            user = update_or_query.from_user
        elif hasattr(update_or_query, 'effective_user'):
            user = update_or_query.effective_user
        else:
            raise ValueError("Неизвестный тип объекта: нет from_user или effective_user")
        logging.info(f"save_meeting: начало, user_id={user.id}, time_str={time_str}")

        fmt = context.user_data.get('meeting_format', 'онлайн')
        city = context.user_data.get('meeting_city', None)
        timestamp = context.user_data.get('meeting_timestamp')
        if not timestamp:
            logging.error("save_meeting: timestamp отсутствует")
            await update_or_query.message.reply_text("Ошибка: не удалось определить время.")
            return MEETING_CONFIRMED

        logging.info(f"save_meeting: сохраняем встречу для user {user.id}, fmt={fmt}, time={time_str}, city={city}, ts={timestamp}")

        db.set_meeting(user.id, fmt, time_str, city, timestamp)

        reply = f"Отлично! Жду тебя {time_str} в формате {fmt} (время московское, МСК)"
        if city:
            reply += f" в городе {city}"
        reply += ".\n\nЯ пришлю напоминание за час. Если планы изменятся – нажми кнопку ниже."
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Перенести встречу", callback_data="reschedule")],
            [InlineKeyboardButton("🏠 В начало", callback_data="return_to_start")]
        ])
        await update_or_query.message.reply_text(reply, reply_markup=keyboard)
        logging.info("save_meeting: сообщение пользователю отправлено")

        inviter_id = db.get_invited_by(user.id)
        if inviter_id:
            inviter_name = db.get_inviter_name(inviter_id) or "друг"
            inviter_username = db.get_username(inviter_id)
            mention = f"@{inviter_username}" if inviter_username else inviter_name

            await context.bot.send_message(
                chat_id=inviter_id,
                text=f"📅 {mention}, твой кандидат {user.first_name} (@{user.username}) записался на встречу.\n"
                     f"Формат: {fmt}, время (МСК): {time_str}{' в городе '+city if city else ''}.\n\n"
                     f"После встречи я спрошу тебя о её результате."
            )
            logging.info(f"save_meeting: уведомление пригласившему {inviter_id} отправлено")
        else:
            await context.bot.send_message(
                chat_id=config.ADMIN_CHAT_ID,
                text=f"📅 Новый кандидат {user.first_name} (@{user.username}) записался на встречу.\n"
                     f"Формат: {fmt}, время (МСК): {time_str}{' в городе '+city if city else ''}."
            )
            logging.info("save_meeting: уведомление админу отправлено")

        await update_or_query.message.reply_text("Я отправил уведомление, скоро с тобой свяжутся.")
        logging.info("save_meeting: завершено успешно, возвращаю MEETING_CONFIRMED")
        return MEETING_CONFIRMED
    except Exception as e:
        logging.error(f"Ошибка в save_meeting: {e}", exc_info=True)
        await update_or_query.message.reply_text("Произошла внутренняя ошибка. Попробуйте позже.")
        return MEETING_CONFIRMED

# -----------------------------------------------------------------------------
# ПЕРЕНОС ВСТРЕЧИ
# -----------------------------------------------------------------------------
async def reschedule_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    logging.info(f"reschedule_handler: получен callback {query.data}")
    await query.answer()
    await query.message.reply_text("В каком ты городе/районе? Напиши, чтобы мы могли договориться о месте встречи.")
    return ASK_CITY

# -----------------------------------------------------------------------------
# ВОЗВРАТ В НАЧАЛО
# -----------------------------------------------------------------------------
async def return_to_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    logging.info(f"return_to_start ВЫЗВАН с callback_data={query.data}")
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("✅ Пройти тест", callback_data="start_test")],
        [InlineKeyboardButton("💰 Хочу узнать про доход", callback_data="start_income")]
    ]
    await query.message.reply_text("Что выберешь? 👇", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOICE

# -----------------------------------------------------------------------------
# БЛОК ПРОДУКТОВ И РЕКОМЕНДАЦИЙ (С ИСТОРИЕЙ)
# -----------------------------------------------------------------------------
async def send_products_and_recommendations(update_or_query, context):
    user = update_or_query.from_user
    inviter_mention = get_inviter_mention(user.id)
    text = (
        f"Окей, понял. Тогда просто знай, что у Альфа-Банка есть полезные продукты, которыми я пользуюсь:\n"
        f"– **Дебетовая карта** с кэшбэком до 30% — сейчас весеннее предложение: 10% на топливо, ремонт дома, супермаркеты, аптеки и 15% на маркетплейсы (Ozon, Wildberries)! Карта бесплатная навсегда.\n"
        f"– **Накопительный счёт** до 16% годовых — деньги работают, даже когда спишь.\n"
        f"– **Программа ПДС** — государство докидывает до 36 000 ₽ в год.\n\n"
        f"А ещё есть **бизнес-карта** с 10% кэшбэка на популярные категории и бонусы до 6000 рублей за первые шаги. "
        f"Каждую пятницу банк дарит по 3000 рублей подарком! 🔥\n\n"
        f"Один мой знакомый просто оформил накопительный счёт, чтобы откладывать на ремонт. Через год он накопил нужную сумму и сказал: «Если бы не эта карта, я бы всё потратил».\n\n"
        f"А если захочешь стать партнёром, откроется доступ к программе **«Всё своё для своих»**:\n"
        f"• 📞 **Бесплатная мобильная связь** — звонки и интернет за счёт банка.\n"
        f"• 🏥 **ДМС до 30 млн рублей** — полное медицинское обслуживание, стоматология, анализы.\n"
        f"• 💰 **+10% к пенсии каждый год** — доплата от твоего заработка в проекте.\n"
        f"• 🎉 **Альфа-Клуб** — мероприятия по всей России, встречи с топ-лидерами.\n"
        f"• 🏆 **Призы и награды** — гаджеты, путешествия, автомобили.\n"
        f"• 🤝 **Сообщество единомышленников** — поддержка, обмен опытом, рост.\n\n"
        f"Если захочешь подробности — пиши, помогу. Мой контакт: {inviter_mention}\n\n"
        f"И если вдруг есть знакомые, кому может быть интересен такой проект — передай, пожалуйста, мой контакт или перешли это сообщение:"
    )
    await update_or_query.message.reply_text(text, parse_mode='Markdown')

    referral = (
        "Привет! Есть человек, который сотрудничает с Альфа-Банком и набирает команду. "
        "Работа удалённо, 1–4 часа в день, доход от 20 000 ₽. Дать контакт?"
    )
    await update_or_query.message.reply_text(
        f"<pre>{referral}</pre>",
        parse_mode='HTML'
    )

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В начало", callback_data="return_to_start")]])
    await update_or_query.message.reply_text(
        "Если хочешь вернуться к выбору, нажми кнопку ниже:",
        reply_markup=keyboard
    )
    return PRODUCTS

# -----------------------------------------------------------------------------
# ПЕРИОДИЧЕСКАЯ ПРОВЕРКА БАЗЫ ДАННЫХ (С ИСТОРИЯМИ В ДОГРЕВЕ)
# -----------------------------------------------------------------------------
async def check_scheduled_tasks(context: ContextTypes.DEFAULT_TYPE):
    now = int(datetime.now().timestamp())
    conn = sqlite3.connect(db.DB_NAME)
    c = conn.cursor()

    # 1. Напоминания о встречах (за час до встречи) – теперь с invited_by
    c.execute('''
        SELECT user_id, meeting_timestamp, first_name, invited_by FROM users
        WHERE meeting_timestamp IS NOT NULL
          AND meeting_timestamp - 3600 <= ? AND meeting_timestamp > ?
          AND meeting_reminder_sent = 0
    ''', (now, now))
    reminders = c.fetchall()
    for user_id, ts, name, inviter_id in reminders:
        tz = pytz.timezone('Europe/Moscow')
        dt = datetime.fromtimestamp(ts, tz=tz).strftime('%d.%m %H:%M')
        # Кандидату – добавляем контакт куратора
        mentor_mention = get_inviter_mention(user_id) if inviter_id else "куратору"
        text_candidate = f"🔔 Напоминаю: через час у тебя встреча {dt} (МСК). Если планы изменились, напиши {mentor_mention} или нажми кнопку переноса."
        await safe_send_message(context, user_id, text_candidate)
        # Куратору – добавляем юзернейм кандидата для точной идентификации
        if inviter_id:
            candidate_username = db.get_username(user_id) or "—"
            text_curator = f"🔔 Напоминание: через час у тебя встреча с {name} (@{candidate_username}) в {dt} (МСК). Будь на связи!"
            await safe_send_message(context, inviter_id, text_curator)
        c.execute("UPDATE users SET meeting_reminder_sent = 1 WHERE user_id = ?", (user_id,))
        conn.commit()

    # 2. Опрос куратора (через 15 минут после встречи)
    c.execute('''
        SELECT user_id, meeting_timestamp, first_name, invited_by FROM users
        WHERE meeting_timestamp IS NOT NULL
          AND meeting_timestamp + 900 <= ?
          AND friend_responded = 0
          AND curator_poll_sent = 0
    ''', (now,))
    to_poll = c.fetchall()
    for user_id, ts, name, friend_id in to_poll:
        if friend_id:
            candidate_username = db.get_username(user_id) or "—"
            text = f"Как прошла встреча с {name} (@{candidate_username})? Готов ли он к партнёрству?"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да, готов", callback_data=f"friend_yes_{user_id}")],
                [InlineKeyboardButton("🔄 Ещё думает", callback_data=f"friend_drip_{user_id}")]
            ])
            await safe_send_message(context, friend_id, text, reply_markup=keyboard)
            c.execute("UPDATE users SET curator_poll_sent = 1 WHERE user_id = ?", (user_id,))
            conn.commit()

    # 3. Догрев (с обновлёнными сообщениями)
    c.execute('''
        SELECT u.user_id, u.meeting_timestamp, u.first_name, u.drip_stage, u.last_drip_time, u.invited_by
        FROM users u
        WHERE u.meeting_timestamp IS NOT NULL
          AND u.meeting_timestamp + 86400*4 <= ?
          AND u.friend_responded = 0
          AND u.drip_stage < 3
    ''', (now,))
    drip_candidates = c.fetchall()
    for user_id, ts, name, stage, last_drip, invited_by in drip_candidates:
        send_now = False
        msg = ""
        keyboard = None
        new_stage = stage

        if stage == 0:
            mentor_mention = get_inviter_mention(user_id) if invited_by else "куратору"
            msg = f"Привет! Как прошла встреча с куратором? Если остались вопросы, напиши {mentor_mention}."
            new_stage = 1
            send_now = True
        elif stage == 1 and last_drip and now - last_drip >= 2*86400:
            mentor_mention = get_inviter_mention(user_id) if invited_by else "куратору"
            msg = (f"Кстати, вспомнил историю: один из моих партнёров очень боялся продавать, думал, что не сможет. "
                   f"А потом просто поделился ссылкой в сторис — и через неделю у него был первый партнёр. "
                   f"Он мне написал: «Оказывается, тут и продавать не надо, просто рассказываешь». "
                   f"Хочешь попробовать? Если да, напиши {mentor_mention}.")
            new_stage = 2
            send_now = True
        elif stage == 2 and last_drip and now - last_drip >= 2*86400:
            msg = (
                "Если пока нет решения по партнёрству, просто посмотри на продукты банка. Они реально выгодные:\n"
                "• 🔥Бизнес-карта — 10% кэшбэка на популярные категории, бонусы до 6000 ₽ и подарки по пятницам.\n"
                "• Дебетовая карта — с весенним кэшбэком 10% на топливо, ремонт, супермаркеты и 15% на маркетплейсы.\n"
                "• Накопительный счёт — до 16% годовых, деньги работают даже когда спишь.\n\n"
                "А если позже захочешь стать партнёром, откроется программа «Всё своё для своих»: "
                "📞бесплатная связь, 🏥ДМС до 30 млн,💰+10% к пенсии, 🤝закрытые мероприятия и сообщество единомышленников.\n\n"
                "Напиши «да», и я расскажу подробнее о продуктах или партнёрстве."
            )
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Да, хочу узнать", callback_data=f"drip_yes_{user_id}")],
                [InlineKeyboardButton("❌ Нет, спасибо", callback_data=f"drip_no_{user_id}")]
            ])
            new_stage = 3
            send_now = True

        if send_now:
            if keyboard:
                await safe_send_message(context, user_id, msg, reply_markup=keyboard)
            else:
                await safe_send_message(context, user_id, msg)
            c.execute('''
                UPDATE users SET drip_stage = ?, last_drip_time = ?
                WHERE user_id = ?
            ''', (new_stage, now, user_id))
            conn.commit()

    conn.close()

# -----------------------------------------------------------------------------
# ОПРОС ДРУГА ПОСЛЕ ВСТРЕЧИ И АВТОДОГРЕВ
# -----------------------------------------------------------------------------
async def friend_yes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    candidate_id = int(query.data.split('_')[-1])
    friend_id = query.from_user.id
    if db.get_invited_by(candidate_id) != friend_id:
        await query.message.reply_text("❌ Вы не можете управлять этим кандидатом.")
        return
    db.set_friend_responded(candidate_id)
    conn = sqlite3.connect(db.DB_NAME)
    c = conn.cursor()
    c.execute('''
        UPDATE users SET drip_stage = 0, last_drip_time = NULL
        WHERE user_id = ?
    ''', (candidate_id,))
    conn.commit()
    conn.close()
    text = "Поздравляю! Ты стал официальным партнёром проекта «Свой в Альфе». Теперь ты можешь приглашать других и пользоваться партнёрскими инструментами."
    await safe_send_message(context, candidate_id, text)
    await query.message.edit_text("✅ Отлично! Кандидат уведомлен.")

async def friend_drip_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    candidate_id = int(query.data.split('_')[-1])
    friend_id = query.from_user.id
    if db.get_invited_by(candidate_id) != friend_id:
        await query.message.reply_text("❌ Вы не можете управлять этим кандидатом.")
        return
    now_ts = int(datetime.now().timestamp())
    conn = sqlite3.connect(db.DB_NAME)
    c = conn.cursor()
    c.execute('''
        UPDATE users SET friend_responded = 1, curator_poll_sent = 1,
        drip_stage = 1, last_drip_time = ?
        WHERE user_id = ?
    ''', (now_ts, candidate_id))
    conn.commit()
    conn.close()
    mentor_mention = get_inviter_mention(candidate_id) if db.get_invited_by(candidate_id) else "куратору"
    msg = f"Привет! Как прошла встреча с куратором? Если остались вопросы, напиши {mentor_mention}."
    await safe_send_message(context, candidate_id, msg)
    await query.message.edit_text("🔄 Догрев запущен. Кандидат будет получать сообщения.")

async def drip_yes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    candidate_id = int(query.data.split('_')[-1])
    inviter_id = db.get_invited_by(candidate_id)
    if inviter_id:
        candidate_name = query.from_user.first_name
        candidate_username = query.from_user.username or "—"
        text = f"👋 {db.get_inviter_name(inviter_id) or ''}, кандидат {candidate_name} (@{candidate_username}) заинтересовался после догрева! Свяжись с ним."
        await safe_send_message(context, inviter_id, text)
        await query.message.edit_text("Отлично! Я передал куратору, он скоро свяжется.")
    else:
        await query.message.edit_text("Не удалось связаться с куратором. Пожалуйста, напишите в поддержку: +7 800 101 0991")
    # сброс догрева
    conn = sqlite3.connect(db.DB_NAME)
    c = conn.cursor()
    c.execute('''
        UPDATE users SET drip_stage = 0, last_drip_time = NULL
        WHERE user_id = ?
    ''', (candidate_id,))
    conn.commit()
    conn.close()

async def drip_no_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    candidate_id = int(query.data.split('_')[-1])
    await send_products_and_recommendations(query, context)
    conn = sqlite3.connect(db.DB_NAME)
    c = conn.cursor()
    c.execute('''
        UPDATE users SET drip_stage = 0, last_drip_time = NULL
        WHERE user_id = ?
    ''', (candidate_id,))
    conn.commit()
    conn.close()
    return PRODUCTS

# -----------------------------------------------------------------------------
# КОМАНДЫ ПАРТНЁРСКОЙ ПАНЕЛИ
# -----------------------------------------------------------------------------
async def invite_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    code = db.ensure_partner_code(user_id)
    invite_link = f"https://t.me/{context.bot.username}?start={user_id}"
    text = (
        f"🔗 **Твоя партнёрская ссылка:**\n"
        f"`{invite_link}`\n\n"
        f"📋 **Твой партнёрский код:**\n"
        f"`{code}`\n\n"
        f"Отправляй эту ссылку друзьям, они начнут общение с ботом и попадут в твою команду."
    )
    await update.message.reply_text(text, parse_mode='Markdown')

async def my_candidates_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    candidates = db.get_candidates(user_id)
    if not candidates:
        text = "У тебя пока нет кандидатов, ожидающих подтверждения."
    else:
        text = "**Кандидаты, готовые к подтверждению:**\n\n"
        for cid, name, username, reg_date in candidates:
            username_disp = f"@{username}" if username else "—"
            text += f"• {name} ({username_disp}) – с {reg_date[:10]}\n"
            text += f"  Подтвердить: `/approve {cid}`\n"
            text += f"  Отклонить: `/reject {cid}`\n\n"
    await update.message.reply_text(text, parse_mode='Markdown')

async def approve_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Использование: /approve <user_id>")
        return
    try:
        candidate_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    inviter_id = db.get_invited_by(candidate_id)
    if inviter_id != user_id:
        await update.message.reply_text("Этот пользователь не ваш кандидат.")
        return
    code = db.approve_partner(candidate_id, user_id)
    await update.message.reply_text(f"✅ Кандидат подтверждён. Его партнёрский код: `{code}`")
    try:
        text = "Поздравляю! Ты стал официальным партнёром проекта «Свой в Альфе». Теперь ты можешь приглашать других и пользоваться партнёрскими инструментами."
        await safe_send_message(update, candidate_id, text)
    except Exception as e:
        logging.error(f"Не удалось уведомить кандидата: {e}")

async def reject_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Использование: /reject <user_id>")
        return
    try:
        candidate_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    inviter_id = db.get_invited_by(candidate_id)
    if inviter_id != user_id:
        await update.message.reply_text("Этот пользователь не ваш кандидат.")
        return
    db.reject_candidate(candidate_id)
    await update.message.reply_text("❌ Кандидат отклонён.")
    try:
        text = "К сожалению, твоя заявка на партнёрство была отклонена."
        await safe_send_message(update, candidate_id, text)
    except:
        pass

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    total, active, pending = db.get_inviter_stats(user_id)
    silent_count = len(db.get_silent_candidates(user_id, days=config.SILENT_DAYS))
    blocked_count = len(db.get_blocked_candidates(user_id))
    inactive_count = len(db.get_inactive_candidates(user_id))
    text = (
        f"📊 **Твоя статистика:**\n\n"
        f"• Приглашено всего: {total}\n"
        f"• Активных партнёров: {active}\n"
        f"• Ожидают подтверждения: {pending}\n"
        f"• Молчунов (>={config.SILENT_DAYS} дней): {silent_count}\n"
        f"• Неактивных (inactive): {inactive_count}\n"
        f"• Заблокировали бота: {blocked_count}\n"
        f"• Доход отображается в партнёрском кабинете Альфа-Банка.\n\n"
        f"Используй /silent, чтобы увидеть список и напомнить.\n"
        f"Используй /blocked, чтобы увидеть заблокировавших."
    )
    await update.message.reply_text(text, parse_mode='Markdown')

async def team_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    partners = db.get_partners(user_id)
    if not partners:
        text = "В твоей команде пока нет активных партнёров."
    else:
        text = "**Твоя команда (активные партнёры):**\n\n"
        for pid, name, username, reg_date in partners:
            username_disp = f"@{username}" if username else "—"
            text += f"• {name} ({username_disp}) – с {reg_date[:10]}\n"
    await update.message.reply_text(text, parse_mode='Markdown')

# -----------------------------------------------------------------------------
# КОМАНДЫ ДЛЯ РАБОТЫ С МОЛЧУНАМИ И СТАТУСАМИ
# -----------------------------------------------------------------------------
def _get_status_description(path, test, feedback, video, meeting):
    if meeting:
        return "📅 встреча назначена"
    if video:
        return "🎥 видео отправлено"
    if feedback:
        return "💬 отзыв получен"
    if test:
        return "✅ тест пройден"
    if path == 'income':
        return "💰 интересовался доходом"
    if path == 'test':
        return "🧐 начал тест"
    return "🆕 новый"

def generate_reminder_message(user_dict, inviter_mention="мне"):
    first_name = user_dict.get('first_name', 'друг')
    path = user_dict.get('chosen_path')
    test_completed = user_dict.get('test_completed')
    feedback = user_dict.get('feedback')
    video_sent = user_dict.get('video_sent')
    video_confirmed = user_dict.get('video_confirmed')
    meeting_time = user_dict.get('meeting_time')
    invited_by = user_dict.get('invited_by')

    if inviter_mention == "мне" and invited_by:
        inviter_mention = get_inviter_mention(invited_by) or "мне"

    if meeting_time:
        return (
            f"Привет, {first_name}! 👋\n"
            f"У тебя была назначена встреча на {meeting_time[:10]}. Не получилось? "
            f"Если хочешь перенести или всё ещё интересно – напиши {inviter_mention}."
        )
    elif video_sent and not video_confirmed:
        return (
            f"Привет, {first_name}! 👋\n"
            f"Тобой не подтверждён просмотр видео. Там как раз про то, как улучшить финансовую ситуацию. "
            f"Если нужна ссылка ещё раз – дай знать."
        )
    elif feedback:
        short_feedback = (feedback[:50] + '...') if len(feedback) > 50 else feedback
        return (
            f"Привет, {first_name}! 👋\n"
            f"Ты пишешь: «{short_feedback}». Мы можем помочь это исправить. "
            f"Если всё ещё актуально – давай продолжим. Напиши {inviter_mention}."
        )
    elif test_completed:
        return (
            f"Привет, {first_name}! 👋\n"
            f"Тобой пройден тест. Что скажешь о результатах? "
            f"Мне правда важно твоё мнение. Напиши {inviter_mention}."
        )
    elif path == 'income':
        return (
            f"Привет, {first_name}! 👋\n"
            f"Тобой проявлен интерес к дополнительному доходу, но пока не до конца. "
            f"Хочешь, я помогу разобраться? Напиши {inviter_mention}."
        )
    else:
        return (
            f"Привет, {first_name}! 👋\n"
            f"Давно не виделись в боте «Свой в Альфе». Если всё ещё интересно – возвращайся, "
            f"пройди тест или посмотри информацию о доходе. Если нужна помощь – напиши {inviter_mention}."
        )

async def silent_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    candidates = db.get_silent_candidates(user_id, days=config.SILENT_DAYS)
    if not candidates:
        await update.message.reply_text("👥 У тебя пока нет неактивных кандидатов.")
        return

    text = f"👤 *Неактивные кандидаты (>={config.SILENT_DAYS} дней):*\n\n"
    keyboard = []
    for cand in candidates:
        cid, name, username, last_active, path, test, feedback, video, meeting = cand
        last_date = datetime.fromisoformat(last_active).strftime("%d.%m %H:%M")
        status = _get_status_description(path, test, feedback, video, meeting)
        line = f"• {name} (@{username})\n  Последняя активность: {last_date}\n  Статус: {status}\n"
        text += line
        keyboard.append([InlineKeyboardButton(f"💬 Напомнить {name}", callback_data=f"remind_{cid}")])

    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    await update.message.reply_text(text, reply_markup=reply_markup)

async def remind_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    candidate_id = int(query.data.split('_')[1])
    mentor_id = query.from_user.id

    inviter_id = db.get_invited_by(candidate_id)
    if inviter_id != mentor_id:
        await query.edit_message_text("❌ Этот кандидат не ваш.")
        return

    user_dict = db.get_user_dict(candidate_id)
    if not user_dict:
        await query.edit_message_text("❌ Кандидат не найден.")
        return

    inviter_mention = get_inviter_mention(candidate_id)
    message = generate_reminder_message(user_dict, inviter_mention)
    success = await safe_send_message(context, candidate_id, message)
    if success:
        await query.edit_message_text(f"✅ Напоминание отправлено {user_dict['first_name'] or ''}.")
    else:
        await query.edit_message_text("❌ Не удалось отправить сообщение (возможно, пользователь заблокировал бота).")

async def blocked_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    blocked = db.get_blocked_candidates(user_id)
    if not blocked:
        await update.message.reply_text("🔇 Нет заблокировавших бота кандидатов.")
        return

    text = "🔇 *Заблокировали бота:*\n\n"
    for bid, name, username, last_active, path in blocked:
        last_date = datetime.fromisoformat(last_active).strftime("%d.%m %H:%M")
        line = f"• {name} (@{username})\n  Последняя активность: {last_date}\n"
        text += line
    await update.message.reply_text(text, parse_mode='Markdown')

async def inactive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    inactive = db.get_inactive_candidates(user_id)
    if not inactive:
        await update.message.reply_text("⏸ Нет пользователей в статусе inactive.")
        return

    text = "⏸ *Неактивные (inactive):*\n\n"
    for bid, name, username, last_active, path in inactive:
        last_date = datetime.fromisoformat(last_active).strftime("%d.%m %H:%M")
        line = f"• {name} (@{username})\n  Последняя активность: {last_date}\n"
        text += line
    await update.message.reply_text(text, parse_mode='Markdown')

# -----------------------------------------------------------------------------
# КОМАНДЫ УПРАВЛЕНИЯ СТАТУСАМИ
# -----------------------------------------------------------------------------
async def block_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Использование: /block <user_id>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    inviter_id = db.get_invited_by(target_id)
    if inviter_id != user_id:
        await update.message.reply_text("❌ Этот пользователь не ваш.")
        return
    db.set_curator_status(target_id, 'blocked')
    db.cancel_user_jobs(target_id, context)
    await update.message.reply_text(f"🔇 Пользователь {target_id} заблокирован.")

async def unblock_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Использование: /unblock <user_id>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    inviter_id = db.get_invited_by(target_id)
    if inviter_id != user_id:
        await update.message.reply_text("❌ Этот пользователь не ваш.")
        return
    db.set_curator_status(target_id, 'active')
    await update.message.reply_text(f"✅ Пользователь {target_id} разблокирован.")

async def make_inactive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Использование: /make_inactive <user_id>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    inviter_id = db.get_invited_by(target_id)
    if inviter_id != user_id:
        await update.message.reply_text("❌ Этот пользователь не ваш.")
        return
    db.set_curator_status(target_id, 'inactive')
    db.cancel_user_jobs(target_id, context)
    await update.message.reply_text(f"⏸ Пользователь {target_id} переведён в неактивные.")

async def activate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Использование: /activate <user_id>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    inviter_id = db.get_invited_by(target_id)
    if inviter_id != user_id:
        await update.message.reply_text("❌ Этот пользователь не ваш.")
        return
    db.set_curator_status(target_id, 'active')
    await update.message.reply_text(f"▶ Пользователь {target_id} активирован.")

# -----------------------------------------------------------------------------
# РУЧНЫЕ КОМАНДЫ ДЛЯ КУРАТОРА ПОСЛЕ ВСТРЕЧИ
# -----------------------------------------------------------------------------
async def approve_after_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Использование: /approve_after <user_id>")
        return
    try:
        candidate_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    inviter_id = db.get_invited_by(candidate_id)
    if inviter_id != user_id:
        await update.message.reply_text("❌ Этот пользователь не ваш кандидат.")
        return
    db.cancel_user_jobs(candidate_id, context)
    code = db.approve_partner(candidate_id, user_id)
    await update.message.reply_text(f"✅ Кандидат подтверждён. Его партнёрский код: `{code}`")
    try:
        await context.bot.send_message(
            chat_id=candidate_id,
            text="Поздравляю! Ты стал официальным партнёром проекта «Свой в Альфе». Теперь ты можешь приглашать других и пользоваться партнёрскими инструментами."
        )
    except Exception as e:
        logging.error(f"Не удалось уведомить кандидата: {e}")

async def reject_after_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("Использование: /reject_after <user_id>")
        return
    try:
        candidate_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверный ID.")
        return
    inviter_id = db.get_invited_by(candidate_id)
    if inviter_id != user_id:
        await update.message.reply_text("❌ Этот пользователь не ваш кандидат.")
        return
    db.cancel_user_jobs(candidate_id, context)
    db.reject_candidate(candidate_id)
    await update.message.reply_text("❌ Кандидат отклонён и переведён в блок продуктов.")
    try:
        await context.bot.send_message(
            chat_id=candidate_id,
            text="К сожалению, твоя заявка на партнёрство была отклонена. Если остались вопросы, свяжись с куратором."
        )
    except Exception as e:
        logging.error(f"Не удалось уведомить кандидата: {e}")

# -----------------------------------------------------------------------------
# КОМАНДА CLAIM
# -----------------------------------------------------------------------------
async def claim_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        candidate_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /claim ID_пользователя")
        return
    friend_id = update.effective_user.id
    if db.get_invited_by(candidate_id) != friend_id:
        await update.message.reply_text("❌ Этот кандидат не ваш.")
        return
    conn = sqlite3.connect(db.DB_NAME)
    c = conn.cursor()
    c.execute('''
        UPDATE users SET drip_stage = 0, last_drip_time = NULL
        WHERE user_id = ?
    ''', (candidate_id,))
    conn.commit()
    conn.close()
    text = "Твой куратор вернулся к общению! Он скоро свяжется."
    await safe_send_message(context, candidate_id, text)
    await update.message.reply_text(f"✅ Управление кандидатом {candidate_id} возвращено. Догрев остановлен.")

# -----------------------------------------------------------------------------
# КОМАНДА HELP
# -----------------------------------------------------------------------------
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "👋 **Доступные команды:**\n\n"
        "/start – начать диалог (если хочешь пройти воронку заново).\n"
        "/invite – получить свою реферальную ссылку и код.\n"
        "/my_candidates – список кандидатов, ожидающих подтверждения.\n"
        "/approve [id] – подтвердить кандидата.\n"
        "/reject [id] – отклонить кандидата.\n"
        "/stats – статистика по приглашённым.\n"
        "/team – список твоих активных партнёров.\n"
        "/silent – показать неактивных кандидатов и напомнить.\n"
        "/blocked – список заблокировавших бота.\n"
        "/inactive – список неактивных (inactive).\n"
        "/block [id] – полностью заблокировать пользователя.\n"
        "/unblock [id] – разблокировать.\n"
        "/make_inactive [id] – перевести в неактивные (исключить из догревов).\n"
        "/activate [id] – вернуть в активное состояние.\n"
        "/claim [id] – вернуть управление кандидатом после автодогрева.\n"
        "/cancel – выйти из текущего диалога.\n\n"
        "Если ты ещё не начал – напиши /start и следуй инструкциям. Удачи! 🚀"
    )
    await update.message.reply_text(help_text, parse_mode='Markdown')

# -----------------------------------------------------------------------------
# ОБРАБОТЧИКИ КНОПОК ПОДТВЕРЖДЕНИЯ ИЗ УВЕДОМЛЕНИЙ
# -----------------------------------------------------------------------------
async def approve_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    candidate_id = int(data.split('_')[1])
    mentor_id = query.from_user.id

    inviter_id = db.get_invited_by(candidate_id)
    if inviter_id != mentor_id:
        await query.edit_message_text("❌ Этот кандидат не ваш.")
        return

    code = db.approve_partner(candidate_id, mentor_id)
    await query.edit_message_text(f"✅ Кандидат подтверждён. Его партнёрский код: `{code}`")

    try:
        text = "Поздравляю! Ты стал официальным партнёром проекта «Свой в Альфе». Теперь ты можешь приглашать других и пользоваться партнёрскими инструментами."
        await safe_send_message(context, candidate_id, text)
    except Exception as e:
        logging.error(f"Не удалось уведомить кандидата: {e}")

async def reject_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    candidate_id = int(query.data.split('_')[1])
    mentor_id = query.from_user.id

    inviter_id = db.get_invited_by(candidate_id)
    if inviter_id != mentor_id:
        await query.edit_message_text("❌ Этот кандидат не ваш.")
        return

    db.reject_candidate(candidate_id)
    await query.edit_message_text("❌ Кандидат отклонён.")

    try:
        text = "К сожалению, твоя заявка на партнёрство была отклонена."
        await safe_send_message(context, candidate_id, text)
    except:
        pass

# -----------------------------------------------------------------------------
# ОТМЕНА
# -----------------------------------------------------------------------------
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Диалог завершён. Чтобы начать заново, напиши /start",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END
