import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ConversationHandler
import config
import database as db
import handlers
from handlers import (CHOICE, ENGAGEMENT_Q1, ENGAGEMENT_Q2, ENGAGEMENT_Q3,
    PRESENTATION_1, PRESENTATION_2, PRESENTATION_3, PRESENTATION_4, PRESENTATION_5,
    AFTER_PRESENTATION, TEST, FEEDBACK, VIDEO_OFFER, WAITING_VIDEO,
    MEETING_FORMAT, MEETING_TIME, CUSTOM_TIME, ASK_CITY, PRODUCTS, MEETING_CONFIRMED)

import warnings
from telegram.warnings import PTBUserWarning
warnings.filterwarnings("ignore", category=PTBUserWarning, module="telegram.ext._conversationhandler")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.error(msg="Exception while handling an update:", exc_info=context.error)
    if update and update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Произошла внутренняя ошибка. Попробуйте позже."
        )

async def fallback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.warning(f"Fallback: получено необработанное сообщение от {update.effective_user.id}: {update.message.text}")
    return None

async def global_block_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверяет статус пользователя. Если заблокирован – отправляет уведомление и прерывает обработку."""
    user = update.effective_user
    if not user:
        return
    status = db.get_curator_status(user.id)
    if status == 'blocked':
        await update.message.reply_text("⛔ Ваш доступ к боту ограничен куратором.")
        return None

def main():
    db.init_db()
    app = ApplicationBuilder().token(config.BOT_TOKEN)\
        .connect_timeout(60).read_timeout(60).write_timeout(60).pool_timeout(60)\
        .build()

    app.add_error_handler(error_handler)

    # Глобальный фильтр блокировки (самый высокий приоритет)
    app.add_handler(MessageHandler(filters.ALL, global_block_filter), group=-1)

    # Глобальные команды (работают всегда)
    app.add_handler(CommandHandler('invite', handlers.invite_command))
    app.add_handler(CommandHandler('my_candidates', handlers.my_candidates_command))
    app.add_handler(CommandHandler('approve', handlers.approve_command))
    app.add_handler(CommandHandler('reject', handlers.reject_command))
    app.add_handler(CommandHandler('stats', handlers.stats_command))
    app.add_handler(CommandHandler('team', handlers.team_command))
    app.add_handler(CommandHandler('claim', handlers.claim_command))
    app.add_handler(CommandHandler('silent', handlers.silent_command))
    app.add_handler(CommandHandler('blocked', handlers.blocked_command))
    app.add_handler(CommandHandler('inactive', handlers.inactive_command))
    app.add_handler(CommandHandler('block', handlers.block_command))
    app.add_handler(CommandHandler('unblock', handlers.unblock_command))
    app.add_handler(CommandHandler('make_inactive', handlers.make_inactive_command))
    app.add_handler(CommandHandler('activate', handlers.activate_command))
    app.add_handler(CommandHandler('help', handlers.help_command))
    app.add_handler(CommandHandler('approve_after', handlers.approve_after_command))
    app.add_handler(CommandHandler('reject_after', handlers.reject_after_command))
    app.add_handler(CallbackQueryHandler(handlers.show_choice, pattern='^show_choice$'))

    # ConversationHandler (обрабатывает диалог)
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', handlers.start)],
        states={
            CHOICE: [CallbackQueryHandler(handlers.choice_handler, pattern='^(start_test|start_income)$')],
            ENGAGEMENT_Q1: [CallbackQueryHandler(handlers.engagement_q1_handler, pattern='^eng_')],
            ENGAGEMENT_Q2: [CallbackQueryHandler(handlers.engagement_q2_handler, pattern='^time_')],
            ENGAGEMENT_Q3: [CallbackQueryHandler(handlers.engagement_q3_handler, pattern='^circle_')],
            PRESENTATION_1: [CallbackQueryHandler(handlers.presentation_next, pattern='^pres_next$')],
            PRESENTATION_2: [CallbackQueryHandler(handlers.presentation_next, pattern='^pres_next$')],
            PRESENTATION_3: [CallbackQueryHandler(handlers.presentation_next, pattern='^pres_next$')],
            PRESENTATION_4: [CallbackQueryHandler(handlers.presentation_next, pattern='^pres_next$')],
            PRESENTATION_5: [CallbackQueryHandler(handlers.presentation_next, pattern='^pres_next$')],
            AFTER_PRESENTATION: [
                CallbackQueryHandler(handlers.partnership_yes, pattern='^partnership_yes$'),
                CallbackQueryHandler(handlers.partnership_no, pattern='^partnership_no$')
            ],
            TEST: [CallbackQueryHandler(handlers.test_answer_handler, pattern='^test_ans_')],
            FEEDBACK: [MessageHandler(filters.TEXT, handlers.receive_feedback)],
            VIDEO_OFFER: [
                CallbackQueryHandler(handlers.video_yes, pattern='^video_yes$'),
                CallbackQueryHandler(handlers.video_no, pattern='^video_no$')
            ],
            WAITING_VIDEO: [
                CallbackQueryHandler(handlers.video_watched, pattern='^video_watched$'),
                CallbackQueryHandler(handlers.video_resend, pattern='^video_resend$')
            ],
            MEETING_FORMAT: [CallbackQueryHandler(handlers.meeting_format_handler, pattern='^meet_')],
            MEETING_TIME: [CallbackQueryHandler(handlers.meeting_time_handler, pattern='^time_')],
            CUSTOM_TIME: [MessageHandler(filters.ALL, handlers.custom_time_handler)],
            ASK_CITY: [MessageHandler(filters.ALL, handlers.ask_city_handler)],
            PRODUCTS: [
                CallbackQueryHandler(handlers.return_to_start, pattern='^return_to_start$'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u,c: ConversationHandler.END)
            ],
            MEETING_CONFIRMED: [
                CallbackQueryHandler(handlers.reschedule_handler, pattern='^reschedule$'),
                CallbackQueryHandler(handlers.return_to_start, pattern='^return_to_start$')
            ],
        },
        fallbacks=[
            CommandHandler('cancel', handlers.cancel),
            MessageHandler(filters.ALL, fallback_handler)
        ],
        allow_reentry=True,
        per_message=False,
        per_chat=True,
        per_user=True
    )

    app.add_handler(conv_handler)

    # Глобальные обработчики callback-запросов (для кнопок вне диалога – надёжный запасной вариант)
    app.add_handler(CallbackQueryHandler(handlers.choice_handler, pattern='^(start_test|start_income)$'))
    # Удалены дублирующие обработчики вовлекающих вопросов
    app.add_handler(CallbackQueryHandler(handlers.presentation_next, pattern='^pres_next$'))
    app.add_handler(CallbackQueryHandler(handlers.video_yes, pattern='^video_yes$'))
    app.add_handler(CallbackQueryHandler(handlers.video_no, pattern='^video_no$'))
    app.add_handler(CallbackQueryHandler(handlers.video_watched, pattern='^video_watched$'))
    app.add_handler(CallbackQueryHandler(handlers.video_resend, pattern='^video_resend$'))
    app.add_handler(CallbackQueryHandler(handlers.meeting_format_handler, pattern='^meet_'))
    app.add_handler(CallbackQueryHandler(handlers.meeting_time_handler, pattern='^time_'))
    app.add_handler(CallbackQueryHandler(handlers.reschedule_handler, pattern='^reschedule$'))
    app.add_handler(CallbackQueryHandler(handlers.friend_yes_handler, pattern='^friend_yes_'))
    app.add_handler(CallbackQueryHandler(handlers.friend_drip_handler, pattern='^friend_drip_'))
    app.add_handler(CallbackQueryHandler(handlers.drip_yes_handler, pattern='^drip_yes_'))
    app.add_handler(CallbackQueryHandler(handlers.drip_no_handler, pattern='^drip_no_'))
    app.add_handler(CallbackQueryHandler(handlers.approve_from_button, pattern='^approve_'))
    app.add_handler(CallbackQueryHandler(handlers.reject_from_button, pattern='^reject_'))
    app.add_handler(CallbackQueryHandler(handlers.return_to_start, pattern='^return_to_start$'))
    app.add_handler(CallbackQueryHandler(handlers.remind_handler, pattern='^remind_'))
    app.add_handler(CallbackQueryHandler(handlers.partnership_yes, pattern='^partnership_yes$'))
    app.add_handler(CallbackQueryHandler(handlers.partnership_no, pattern='^partnership_no$'))

    # --- Запуск периодической проверки базы данных ---
    job_queue = app.job_queue
    if job_queue:
        job_queue.run_repeating(handlers.check_scheduled_tasks, interval=60, first=10)
        logging.info("✅ Запущена периодическая проверка базы данных (интервал 60 сек)")
    else:
        logging.warning("JobQueue не доступен, периодическая проверка не запущена")

    app.run_polling()

if __name__ == '__main__':
    main()