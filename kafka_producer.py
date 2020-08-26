from tkinter import messagebox
from tkinter import *
from tkinter.ttk import *
from kafka import KafkaProducer
from datetime import datetime
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
import sqlite3
import json
import xmlformatter
db_name = 'config.db'
message_timestamp = None

class SqlliteConn(): 
    def __init__(self, db_name): 
        self.db_name = db_name 
        self.connection = None
  
    def __enter__(self): 
        self.connection = sqlite3.connect(self.db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        return self.connection
  
    def __exit__(self, exc_type, exc_value, exc_traceback): 
        self.connection.commit()
        self.connection.close()

def send_to_kafka(servers, topic, key, value, outtxt):
    try:
        producer = KafkaProducer(bootstrap_servers=servers)
        producer.send(topic, key=str(key).encode('utf-8'), value=str(value).encode('utf-8'))
        outtxt.insert(END, str(datetime.now()) + ': ' + 'Message sent' + '\n')
        outtxt.see(END)
        producer.close()
    except NoBrokersAvailable as e:
        print(str(e))
        outtxt.insert(END, str(datetime.now()) + ': ' + str(e) + '\n')
        outtxt.see(END)
    except KafkaTimeoutError as e:
        print(str(e))
        outtxt.insert(END, str(datetime.now()) + ': ' + str(e) + '\n')
        outtxt.see(END)

def init_db():
    with SqlliteConn(db_name=db_name) as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS brokers (value TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT brokers_pk PRIMARY KEY (value));')
        conn.execute('CREATE TABLE IF NOT EXISTS topics (value TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT topics_pk PRIMARY KEY (value));')
        conn.execute('CREATE TABLE IF NOT EXISTS keys (value TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT keys_pk PRIMARY KEY (value));')
        conn.execute('CREATE TABLE IF NOT EXISTS messages (value TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT messages_pk PRIMARY KEY (value));')

def get_list(type):
    l = []
    with SqlliteConn(db_name=db_name) as conn:
        cursor = conn.execute('select value from %s order by timestamp desc' % type)
        for r in cursor:
            l.append(r[0])
    return l

def relenght_tables():
    maxlen = 20
    with SqlliteConn(db_name=db_name) as conn:
        conn.execute('delete from brokers where timestamp in (select b.timestamp from brokers b left join (select timestamp from brokers order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')
        conn.execute('delete from topics where timestamp in (select b.timestamp from topics b left join (select timestamp from topics order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')
        conn.execute('delete from keys where timestamp in (select b.timestamp from keys b left join (select timestamp from keys order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')
        conn.execute('delete from messages where timestamp in (select b.timestamp from messages b left join (select timestamp from messages order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')

def send():
    _broker = str(servers.get()).strip()
    _topic = str(topic.get()).strip()
    _key = str(key.get()).strip()
    _value = str(value.get('1.0', 'end-1c')).strip()
    with SqlliteConn(db_name=db_name) as conn:
        conn.execute('insert or replace into brokers(value) values(?)', (_broker,))
        conn.execute('insert or replace into topics(value) values(?)', (_topic,))
        conn.execute('insert or replace into keys(value) values(?)', (_key,))
        conn.execute('insert or replace into messages(value) values(?)', (_value,))
    relenght_tables()
    update_lists()
    if len(_broker)>0 and len(_topic)>0 and len(_key)>0 and len(_value)>0:
        send_to_kafka(_broker, _topic, _key, _value, outtxt)

def get_latest_msg():
    with SqlliteConn(db_name=db_name) as conn:
        cursor = conn.execute('select value,timestamp from messages order by timestamp desc limit 1')
        for r in cursor:
            return (r[0], r[1])
    return (None,None)

def get_prev_msg(timestamp):
    with SqlliteConn(db_name=db_name) as conn:
        cursor = conn.execute('select value,timestamp from messages where timestamp < ? order by timestamp desc limit 1', (timestamp,))
        for r in cursor:
            return (r[0], r[1])
    return (None,None)

def get_next_msg(timestamp):
    with SqlliteConn(db_name=db_name) as conn:
        cursor = conn.execute('select value,timestamp from messages where timestamp > ? order by timestamp asc limit 1', (timestamp,))
        for r in cursor:
            return (r[0], r[1])
    return (None,None)

def update_lists():
    servers['values'] = get_list('brokers')
    if len(servers['values'])>0:
        servers.current(0)
    topic['values'] = get_list('topics')
    if len(topic['values'])>0:
        topic.current(0)
    key['values'] = get_list('keys')
    if len(key['values'])>0:
        key.current(0)

def update_message():
    msg,ts = get_latest_msg()
    if ts is None:
        return
    value.delete('1.0', END)
    value.insert(END, msg)
    global message_timestamp
    message_timestamp = ts

def set_prev_msg():
    global message_timestamp
    msg,ts = get_prev_msg(message_timestamp)
    if ts is not None:
        message_timestamp = ts
        value.delete('1.0', END)
        value.insert(END, msg)
def format_json():
    try:
        _value = str(value.get('1.0', 'end-1c')).strip()
        j = json.loads(_value)
        _value = json.dumps(j, indent=2)
        value.delete('1.0', END)
        value.insert(END, _value)
    except:
        outtxt.insert(END, str(datetime.now()) + ': ' + str('Unable to format as json') + '\n')
        outtxt.see(END)
def format_xml():
    try:
        _value = str(value.get('1.0', 'end-1c')).strip()
        formatter = xmlformatter.Formatter(indent="1", indent_char="\t", encoding_output="UTF-8", preserve=["literal"])
        _value = formatter.format_string(_value)
        if _value:
            value.delete('1.0', END)
            value.insert(END, _value)
    except:
        None
def set_next_msg():
    global message_timestamp
    msg,ts = get_next_msg(message_timestamp)
    if ts is not None:
        message_timestamp = ts
        value.delete('1.0', END)
        value.insert(END, msg)
def on_closing():
    if messagebox.askokcancel("Quit", "Do you want to quit?"):
        main_window.destroy()

init_db()
main_window = Tk()
main_window.title('Kafka Producer by Kulwinderjit')
main_window.protocol("WM_DELETE_WINDOW", on_closing)
buttons_frame = Frame(main_window)
servers_label = LabelFrame(buttons_frame, text='Bootstrap servers', padding='2 2 2 2')
servers = Combobox(servers_label)
servers.grid(row=0, column=0, sticky=N+W+E)
servers_label.grid(row=0, column=0, padx=5, sticky=N+W+E)
servers_label.columnconfigure(0, weight=1)

topic_label = LabelFrame(buttons_frame, text='Topic', padding='2 2 2 2')
topic = Combobox(topic_label)
topic.grid(row=0, column=0, sticky=N+W+E)
topic_label.grid(row=1, column=0, padx=5, sticky=N+W+E)
topic_label.columnconfigure(0, weight=1)

key_label = LabelFrame(buttons_frame, text='Key', padding='2 2 2 2')
key = Combobox(key_label)
key.grid(row=0, column=0, sticky=N+W+E)
key_label.grid(row=2, column=0, padx=5, sticky=N+W+E)
key_label.columnconfigure(0, weight=1)

send_button = Button(buttons_frame, text='Send', command= lambda: send())
send_button.grid(row=2, column=1, sticky=S+W+E)

buttons_frame.grid(row=0, column=0, sticky=N+W+E)
buttons_frame.columnconfigure(0, weight=2)
buttons_frame.columnconfigure(1, weight=1)
buttons_frame.columnconfigure(2, weight=2)

value_label = LabelFrame(main_window, text='Message', padding='2 2 2 2')
value = Text(value_label, wrap=NONE, undo=True, maxundo=-1, autoseparators=True)
scrollb = Scrollbar(value_label, command=value.yview)
scrollb_h = Scrollbar(value_label, command=value.xview, orient=HORIZONTAL)
value['yscrollcommand'] = scrollb.set
value['xscrollcommand'] = scrollb_h.set
value.grid(row=0, column=0, padx=2, pady=2, sticky=N+W+E)
value_label.grid(row=1, column=0, padx=5, sticky=N+W+E)
value_label.columnconfigure(0, weight=1)
value_label.rowconfigure(0, weight=1)
scrollb.grid(row=0, column=1, sticky=N+S)
scrollb_h.grid(row=1, column=0, sticky=E+W)
next_value = Button(value, text='>', padding='0 0 0 0', width=4, command=set_next_msg, cursor='arrow')
next_value.place(relx=1.0, rely=1.0, x=-2, y=-2,anchor="se")
prev_value = Button(value, text='<', padding='0 0 0 0', width=4, command=set_prev_msg, cursor='arrow')
prev_value.place(relx=1.0, rely=1.0, x=-35, y=-2,anchor="se")
json_button = Button(value, text='toJson', padding='0 0 0 0', width=6, command=format_json, cursor='arrow')
json_button.place(relx=1.0, rely=1.0, x=-68, y=-2,anchor="se")
xml_button = Button(value, text='toXml', padding='0 0 0 0', width=6, command=format_xml, cursor='arrow')
xml_button.place(relx=1.0, rely=1.0, x=-113, y=-2,anchor="se")

outtxt_label = LabelFrame(main_window, text='Log', padding='2 2 2 2')
outtxt = Text(outtxt_label)
scrollb = Scrollbar(outtxt_label, command=outtxt.yview)
outtxt['yscrollcommand'] = scrollb.set
outtxt.grid(row=0, column=0, sticky=N+W+E)
outtxt_label.grid(row=2, column=0, padx=5, sticky=N+W+E)
outtxt_label.columnconfigure(0, weight=1)
outtxt_label.rowconfigure(0, weight=1)
scrollb.grid(row=0, column=1, sticky='nsew')

main_window.rowconfigure(0, weight=1, minsize=135)
main_window.rowconfigure(1, weight=1)
main_window.rowconfigure(2, weight=4)
main_window.columnconfigure(0, weight=1)
main_window.style = Style()
main_window.geometry('800x600')
update_lists()
update_message()
main_window.style.theme_use("vista")
main_window.mainloop()
