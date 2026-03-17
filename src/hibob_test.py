from hibob_client import HiBobClient

# Initialize client
client = HiBobClient("SERVICE-33880", "Et4Gy4oUD8DMTW7mAm01c5te4x2QEny70WEb81g5")

# Get all employees
df_employees = client.get_all_employees([
    "firstName", "surname", "email", "work.department", "work.title"
])
print(df_employees.head())
print(df_employees.shape)
