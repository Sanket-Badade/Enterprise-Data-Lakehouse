#!/usr/bin/env python
# coding: utf-8

# ## Simulates a source system by sending JSON data to Azure Event Hub.

# In[1]:


import json, time, random
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData


# ## Event hub Configuration

# In[7]:


connection_string = "***************"
eventhub_name = "******"

customers = {
    101: {"name": "Rahul Sharma", "email": "rahul@gmail.com", "city": "Pune", "state": "Maharashtra"},
    102: {"name": "Priya Verma", "email": "priya@gmail.com", "city": "Mumbai", "state": "Maharashtra"},
    103: {"name": "Amit Singh", "email": "amit@gmail.com", "city": "Delhi", "state": "Delhi"},
    104: {"name": "Sneha Patil", "email": "sneha@gmail.com", "city": "Pune", "state": "Maharashtra"},
    105: {"name": "Arjun Mehta", "email": "arjun@gmail.com", "city": "Ahmedabad", "state": "Gujarat"},

    106: {"name": "Karan Joshi", "email": "karan@gmail.com", "city": "Nagpur", "state": "Maharashtra"},
    107: {"name": "Neha Kapoor", "email": "neha@gmail.com", "city": "Jaipur", "state": "Rajasthan"},
    108: {"name": "Rohit Das", "email": "rohit@gmail.com", "city": "Kolkata", "state": "West Bengal"},
    109: {"name": "Pooja Nair", "email": "pooja@gmail.com", "city": "Kochi", "state": "Kerala"},
    110: {"name": "Vikram Rao", "email": "vikram@gmail.com", "city": "Hyderabad", "state": "Telangana"},

    111: {"name": "Anjali Desai", "email": "anjali@gmail.com", "city": "Surat", "state": "Gujarat"},
    112: {"name": "Manish Yadav", "email": "manish@gmail.com", "city": "Lucknow", "state": "Uttar Pradesh"},
    113: {"name": "Divya Iyer", "email": "divya@gmail.com", "city": "Chennai", "state": "Tamil Nadu"},
    114: {"name": "Saurabh Jain", "email": "saurabh@gmail.com", "city": "Indore", "state": "Madhya Pradesh"},
    115: {"name": "Meera Kulkarni", "email": "meera@gmail.com", "city": "Nashik", "state": "Maharashtra"},

    116: {"name": "Nitin Gupta", "email": "nitin@gmail.com", "city": "Kanpur", "state": "Uttar Pradesh"},
    117: {"name": "Riya Sen", "email": "riya@gmail.com", "city": "Kolkata", "state": "West Bengal"},
    118: {"name": "Aditya Roy", "email": "aditya@gmail.com", "city": "Bhopal", "state": "Madhya Pradesh"},
    119: {"name": "Kavya Menon", "email": "kavya@gmail.com", "city": "Trivandrum", "state": "Kerala"},
    120: {"name": "Harsh Patel", "email": "harsh@gmail.com", "city": "Rajkot", "state": "Gujarat"},

    121: {"name": "Simran Kaur", "email": "simran@gmail.com", "city": "Amritsar", "state": "Punjab"},
    122: {"name": "Yash Thakur", "email": "yash@gmail.com", "city": "Shimla", "state": "Himachal Pradesh"},
    123: {"name": "Tanvi Shah", "email": "tanvi@gmail.com", "city": "Vadodara", "state": "Gujarat"},
    124: {"name": "Deepak Mishra", "email": "deepak@gmail.com", "city": "Patna", "state": "Bihar"},
    125: {"name": "Isha Malhotra", "email": "isha@gmail.com", "city": "Chandigarh", "state": "Punjab"},

    126: {"name": "Aakash Verma", "email": "aakash@gmail.com", "city": "Noida", "state": "Uttar Pradesh"},
    127: {"name": "Bhavna Rao", "email": "bhavna@gmail.com", "city": "Mysore", "state": "Karnataka"},
    128: {"name": "Chirag Soni", "email": "chirag@gmail.com", "city": "Udaipur", "state": "Rajasthan"},
    129: {"name": "Disha Arora", "email": "disha@gmail.com", "city": "Ludhiana", "state": "Punjab"},
    130: {"name": "Farhan Ali", "email": "farhan@gmail.com", "city": "Aligarh", "state": "Uttar Pradesh"},

    131: {"name": "Gauri Joshi", "email": "gauri@gmail.com", "city": "Pune", "state": "Maharashtra"},
    132: {"name": "Hemant Rawat", "email": "hemant@gmail.com", "city": "Dehradun", "state": "Uttarakhand"},
    133: {"name": "Irfan Khan", "email": "irfan@gmail.com", "city": "Bhopal", "state": "Madhya Pradesh"},
    134: {"name": "Juhi Chawla", "email": "juhi@gmail.com", "city": "Delhi", "state": "Delhi"},
    135: {"name": "Kunal Bansal", "email": "kunal@gmail.com", "city": "Jaipur", "state": "Rajasthan"},

    136: {"name": "Lavanya Reddy", "email": "lavanya@gmail.com", "city": "Warangal", "state": "Telangana"},
    137: {"name": "Mohit Arora", "email": "mohit@gmail.com", "city": "Jalandhar", "state": "Punjab"},
    138: {"name": "Naina Singh", "email": "naina@gmail.com", "city": "Varanasi", "state": "Uttar Pradesh"},
    139: {"name": "Omkar Patil", "email": "omkar@gmail.com", "city": "Kolhapur", "state": "Maharashtra"},
    140: {"name": "Parul Mehta", "email": "parul@gmail.com", "city": "Ahmedabad", "state": "Gujarat"},

    141: {"name": "Qasim Sheikh", "email": "qasim@gmail.com", "city": "Aurangabad", "state": "Maharashtra"},
    142: {"name": "Rakesh Kumar", "email": "rakesh@gmail.com", "city": "Ranchi", "state": "Jharkhand"},
    143: {"name": "Sakshi Jain", "email": "sakshi@gmail.com", "city": "Gwalior", "state": "Madhya Pradesh"},
    144: {"name": "Tarun Malhotra", "email": "tarun@gmail.com", "city": "Faridabad", "state": "Haryana"},
    145: {"name": "Umesh Naik", "email": "umesh@gmail.com", "city": "Goa", "state": "Goa"},

    146: {"name": "Vaishnavi Iyer", "email": "vaishnavi@gmail.com", "city": "Chennai", "state": "Tamil Nadu"},
    147: {"name": "Wasim Akhtar", "email": "wasim@gmail.com", "city": "Srinagar", "state": "Jammu and Kashmir"},
    148: {"name": "Xavier Dsouza", "email": "xavier@gmail.com", "city": "Mangalore", "state": "Karnataka"},
    149: {"name": "Yamini Kulkarni", "email": "yamini@gmail.com", "city": "Nagpur", "state": "Maharashtra"},
    150: {"name": "Zaid Khan", "email": "zaid@gmail.com", "city": "Lucknow", "state": "Uttar Pradesh"},

    151: {"name": "Abhishek Tiwari", "email": "abhishek@gmail.com", "city": "Kanpur", "state": "Uttar Pradesh"},
    152: {"name": "Bhavesh Patel", "email": "bhavesh@gmail.com", "city": "Surat", "state": "Gujarat"},
    153: {"name": "Chetan Sharma", "email": "chetan@gmail.com", "city": "Delhi", "state": "Delhi"},
    154: {"name": "Damini Roy", "email": "damini@gmail.com", "city": "Kolkata", "state": "West Bengal"},
    155: {"name": "Esha Nair", "email": "esha@gmail.com", "city": "Kochi", "state": "Kerala"},

    156: {"name": "Faisal Ahmed", "email": "faisal@gmail.com", "city": "Hyderabad", "state": "Telangana"},
    157: {"name": "Gopal Verma", "email": "gopal@gmail.com", "city": "Patna", "state": "Bihar"},
    158: {"name": "Hina Kapoor", "email": "hina@gmail.com", "city": "Mumbai", "state": "Maharashtra"},
    159: {"name": "Imran Shaikh", "email": "imran@gmail.com", "city": "Pune", "state": "Maharashtra"},
    160: {"name": "Jaya Menon", "email": "jaya@gmail.com", "city": "Trivandrum", "state": "Kerala"},

    161: {"name": "Kishore Rao", "email": "kishore@gmail.com", "city": "Bangalore", "state": "Karnataka"},
    162: {"name": "Leena Shah", "email": "leena@gmail.com", "city": "Vadodara", "state": "Gujarat"},
    163: {"name": "Mahesh Yadav", "email": "mahesh@gmail.com", "city": "Noida", "state": "Uttar Pradesh"},
    164: {"name": "Nidhi Arora", "email": "nidhi@gmail.com", "city": "Chandigarh", "state": "Punjab"},
    165: {"name": "Ojas Mehta", "email": "ojas@gmail.com", "city": "Rajkot", "state": "Gujarat"},

    166: {"name": "Pankaj Singh", "email": "pankaj@gmail.com", "city": "Varanasi", "state": "Uttar Pradesh"},
    167: {"name": "Queen Dsouza", "email": "queen@gmail.com", "city": "Goa", "state": "Goa"},
    168: {"name": "Rahul Nair", "email": "rahul.nair@gmail.com", "city": "Kochi", "state": "Kerala"},
    169: {"name": "Shivam Joshi", "email": "shivam@gmail.com", "city": "Dehradun", "state": "Uttarakhand"},
    170: {"name": "Tina Kapoor", "email": "tina@gmail.com", "city": "Delhi", "state": "Delhi"},

    171: {"name": "Utkarsh Jain", "email": "utkarsh@gmail.com", "city": "Indore", "state": "Madhya Pradesh"},
    172: {"name": "Vivek Desai", "email": "vivek@gmail.com", "city": "Ahmedabad", "state": "Gujarat"},
    173: {"name": "Waseem Ali", "email": "waseem@gmail.com", "city": "Lucknow", "state": "Uttar Pradesh"},
    174: {"name": "Yogita Sharma", "email": "yogita@gmail.com", "city": "Jaipur", "state": "Rajasthan"},
    175: {"name": "Zarina Sheikh", "email": "zarina@gmail.com", "city": "Mumbai", "state": "Maharashtra"}
}

products = {
    501: {"product_name": "Laptop", "category": "Electronics", "price": 55000, "brand": "Dell"},
    502: {"product_name": "Mobile", "category": "Electronics", "price": 20000, "brand": "Samsung"},
    503: {"product_name": "Headphones", "category": "Accessories", "price": 2000, "brand": "Sony"},
    504: {"product_name": "Smart Watch", "category": "Electronics", "price": 7000, "brand": "Noise"},
    505: {"product_name": "Keyboard", "category": "Accessories", "price": 1500, "brand": "Logitech"},
    506: {"product_name": "Mouse", "category": "Accessories", "price": 800, "brand": "HP"},
    507: {"product_name": "Tablet", "category": "Electronics", "price": 30000, "brand": "Apple"},
    508: {"product_name": "Printer", "category": "Electronics", "price": 12000, "brand": "Canon"},
    509: {"product_name": "Monitor", "category": "Electronics", "price": 15000, "brand": "LG"},
    510: {"product_name": "Speaker", "category": "Accessories", "price": 3500, "brand": "JBL"},
    511: {"product_name": "Camera", "category": "Electronics", "price": 45000, "brand": "Nikon"},
    512: {"product_name": "Power Bank", "category": "Accessories", "price": 1800, "brand": "Mi"},
    513: {"product_name": "Router", "category": "Electronics", "price": 2500, "brand": "TP-Link"},
    514: {"product_name": "SSD", "category": "Computer Parts", "price": 6000, "brand": "Samsung"},
    515: {"product_name": "Hard Disk", "category": "Computer Parts", "price": 5000, "brand": "Seagate"},
    516: {"product_name": "RAM", "category": "Computer Parts", "price": 4000, "brand": "Corsair"},
    517: {"product_name": "Graphics Card", "category": "Computer Parts", "price": 35000, "brand": "NVIDIA"},
    518: {"product_name": "Microphone", "category": "Accessories", "price": 2500, "brand": "Blue Yeti"},
    519: {"product_name": "Projector", "category": "Electronics", "price": 28000, "brand": "Epson"},
    520: {"product_name": "Gaming Console", "category": "Electronics", "price": 50000, "brand": "Sony"}
}


# In[8]:


def simulate_scd_change(customer_id):
    cust = customers[customer_id].copy()
    if random.random() < 0.3: # 30% chance of a move to test SCD Type 2
        cust["city"] = random.choice(["Bangalore", "Hyderabad", "Chennai"])
        cust["state"] = "Other"
    return cust


# In[9]:


def generate_order_record(order_id_num):
    cid = random.choice(list(customers.keys()))
    pid = random.choice(list(products.keys()))
    c_info = simulate_scd_change(cid)
    p_info = products[pid]
    qty = random.randint(1, 5)
    return {
        "order_id": order_id_num,
        "customer_id": cid, "customer_name": c_info["name"], "email": c_info["email"],
        "city": c_info["city"], "state": c_info["state"],
        "product_id": pid, "product_name": p_info["product_name"],
        "category": p_info["category"], "price": p_info["price"], "brand": p_info["brand"],
        "quantity": qty, "total_amount": qty * p_info["price"],
        "order_date": datetime.now().strftime("%Y-%m-%d")
    }


# In[10]:


def run_controlled_producer(start_id, end_id, batch_size=150):
    producer = EventHubProducerClient.from_connection_string( conn_str=connection_string, eventhub_name=eventhub_name)
    curr_id = start_id
    try:
        while curr_id <= end_id:
            batch = producer.create_batch()
            for _ in range(batch_size):
                if curr_id > end_id: break
                batch.add(EventData(json.dumps(generate_order_record(curr_id))))
                curr_id += 1
            producer.send_batch(batch)
            print(f"Sent up to ORD_{curr_id-1}"); time.sleep(0.1)
    finally: producer.close()



# In[17]:


run_controlled_producer(180750, 200000)


# In[ ]:




