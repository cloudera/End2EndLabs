import random
import uuid
from datetime import datetime
import pandas as pd

df = pd.read_csv("data/customer.csv")
df.rename(columns={"phone number": "customer_id"}, inplace=True)

customer_ids = df["customer_id"].unique().tolist()

tower_ids = [f"TOWER_{i}" for i in range(1, 21)]

def generate_cdr():
    caller = random.choice(customer_ids)
    receiver = random.choice(customer_ids)

    while receiver == caller:
        receiver = random.choice(customer_ids)

    signal = random.randint(-110, -65)

    call_result = "DROPPED" if signal < -95 and random.random() > 0.4 else "SUCCESS"

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "caller_id": caller,
        "receiver_id": receiver,
        "duration": random.randint(10, 300),
        "cell_tower_id": random.choice(tower_ids),
        "call_result": call_result,
        "signal_strength": signal
    }