import random
from datetime import datetime, timedelta

teams = ["Support Tier 1", "Support Tier 2", "Billing", "Tech Ops"]

with open("insert_data_bulk.sql", "w", encoding="utf-8") as f:
    f.write("USE hw2_dw;\n\n")
    
    # 1. Множинна вставка працівників
    f.write("-- Bulk вставка працівників\n")
    f.write("INSERT INTO employees (full_name, team) VALUES\n")
    
    employee_values = []
    for i in range(1, 51):
        name = f"Employee_{i}"
        team = random.choice(teams)
        employee_values.append(f"('{name}', '{team}')")
    
    # З'єднуємо всі значення через кому і ставимо крапку з комою в кінці
    f.write(",\n".join(employee_values) + ";\n\n")
    
    # 2. Множинна вставка дзвінків
    f.write("-- Bulk вставка дзвінків\n")
    f.write("INSERT INTO calls (employee_id, call_time, phone, direction, status) VALUES\n")
    
    base_time = datetime.now() - timedelta(days=5)
    call_values = []
    
    for call_id in range(1, 201):
        emp_id = random.randint(1, 50)
        base_time += timedelta(minutes=random.randint(1, 60))
        call_time_str = base_time.strftime('%Y-%m-%d %H:%M:%S')
        phone = f"+380{random.randint(100000000, 999999999)}"
        direction = random.choice(["Inbound", "Outbound"])
        status = random.choice(["Completed", "Dropped", "Missed"])
        
        call_values.append(f"({emp_id}, '{call_time_str}', '{phone}', '{direction}', '{status}')")
        
    f.write(",\n".join(call_values) + ";\n")

print("Файл insert_data_bulk.sql успішно згенеровано! Запусти його в MySQL.")

