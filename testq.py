import pandas as pd

# Создаем тестовый датафрейм
df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4],
        "значение": ["зн1_тест", "зн3_старт", "зн2_финал", "другое_зн1"],
    }
)

# Используем .str.startswith() с кортежем значений
result = df.query("значение.str.contains('^(?:зн1|зн2)')")

print(result)
