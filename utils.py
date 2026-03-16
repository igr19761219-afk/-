import logging
from questions import questions

def calculate_score(answers):
    """answers: список индексов выбранных вариантов (0-3)"""
    total = 0
    category_scores = {}
    # Защита от несовпадения длины
    min_len = min(len(answers), len(questions))
    for i in range(min_len):
        ans = answers[i]
        total += ans
        cat = questions[i][2]
        category_scores[cat] = category_scores.get(cat, 0) + ans
    return total, category_scores

def interpret_score(total, category_scores):
    if total <= 10:
        level = "Низкий"
    elif total <= 20:
        level = "Средний"
    else:
        level = "Высокий"

    cat_names = {
        'control': 'учёт и контроль',
        'savings': 'сбережения',
        'debts': 'кредиты',
        'invest': 'инвестиции',
        'planning': 'планирование',
    }
    # Исключаем мотивацию из слабых категорий
    relevant = {k: v for k, v in category_scores.items() if k != 'motivation'}
    if not relevant:
        weak = "финансовую подушку"
    else:
        min_cat = min(relevant, key=relevant.get)
        weak = cat_names.get(min_cat, min_cat)

    result = f"**Общий уровень финансовой грамотности: {level}**\n\n"
    result += "Результаты по категориям:\n"
    for cat, score in category_scores.items():
        if cat in cat_names:
            result += f"- {cat_names[cat]}: {score} баллов\n"
    result += f"\nОсобенно стоит обратить внимание на **{weak}**."
    return result, weak