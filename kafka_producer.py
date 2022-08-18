from tkinter import messagebox
from tkinter import *
from tkinter.ttk import *
from kafka import KafkaProducer
from datetime import datetime
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
import sqlite3
import json
import xmlformatter
from datetime import datetime
from datetime import timedelta
from math import floor
import binascii

db_name = 'producer_config.db'
version = '2'

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

def relength_tables():
    maxlen = 20
    with SqlliteConn(db_name=db_name) as conn:
        conn.execute('delete from brokers where timestamp in (select b.timestamp from brokers b left join (select timestamp from brokers order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')
        conn.execute('delete from topics where timestamp in (select b.timestamp from topics b left join (select timestamp from topics order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')
        conn.execute('delete from keys where timestamp in (select b.timestamp from keys b left join (select timestamp from keys order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')
        conn.execute('delete from messages where timestamp in (select b.timestamp from messages b left join (select timestamp from messages order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')

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

class About:
    def __init__(self, parent):
        self.tp = Toplevel(parent, background='black')
        self.tp.transient(parent)
        self.tp.grab_set()
        self.tp.title('About')
        w = 300
        h = 100
        self.tp.resizable(0, 0)
        ws = parent.winfo_screenwidth() # width of the screen
        hs = parent.winfo_screenheight() # height of the screen
        x = (ws/2) - (w/2)
        y = (hs/2) - (h/2)
        self.tp.geometry('%dx%d+%d+%d' % (w, h, x, y))
        self.tp.bind("<Return>", self.ok)
        self.tp.bind("<Escape>", self.ok)
        l = Label(self.tp, text='Created by Kulwinderjit Singh', anchor=CENTER, font='Helvetica 14', foreground='green', background='black')
        l.pack()
        l2 = Label(self.tp, text='in python', anchor=CENTER, font='Helvetica 14', foreground='green', background='black')
        l2.pack()
        l2 = Label(self.tp, text='Version ' + version, anchor=CENTER, font='Helvetica 14', foreground='green', background='black')
        l2.pack()
    def ok(self, event=None):
        self.tp.destroy()

class ProducerUI:
    def __init__(self, tab, root) -> None:
        self.message_timestamp = None
        self.outtxt_index = 0
        self.main_window = root
        window_frame = Frame(tab)
        left_frame = Frame(window_frame)
        buttons_frame = Frame(left_frame)
        servers_label = LabelFrame(buttons_frame, text='Bootstrap servers', padding='2 2 2 2')
        self.servers = Combobox(servers_label)
        self.servers.grid(row=0, column=0, sticky=N+W+E)
        servers_label.grid(row=0, column=0, padx=5, sticky=N+W+E)
        servers_label.columnconfigure(0, weight=1)
        about_btn = Button(buttons_frame, text='About', command=self.show_about)
        about_btn.grid(row=0, column=3, sticky=N+E, padx=5, pady=5)

        topic_label = LabelFrame(buttons_frame, text='Topic', padding='2 2 2 2')
        self.topic = Combobox(topic_label)
        self.topic.grid(row=0, column=0, sticky=N+W+E)
        topic_label.grid(row=1, column=0, padx=5, sticky=N+W+E)
        topic_label.columnconfigure(0, weight=1)

        key_label = LabelFrame(buttons_frame, text='Key', padding='2 2 2 2')
        self.key = Combobox(key_label)
        self.key.grid(row=0, column=0, sticky=N+W+E)
        key_label.grid(row=2, column=0, padx=5, sticky=N+W+E)
        key_label.columnconfigure(0, weight=1)

        send_buttons_frame = Frame(buttons_frame)
        send_button = Button(send_buttons_frame, text='Save Send', command= lambda: self.send())
        send_button.grid(row=0, column=0, sticky=S+W)
        nosave_send_button = Button(send_buttons_frame, text='Send', command= lambda: self.send(False))
        nosave_send_button.grid(row=0, column=1, sticky=S+W)
        send_buttons_frame.grid(row=2, column=1, sticky=S+W)

        buttons_frame.grid(row=0, column=0, sticky=N+W+E+S)
        buttons_frame.columnconfigure(0, weight=1)

        value_label = LabelFrame(left_frame, text='Message', padding='2 2 2 2')
        self.value = Text(value_label, wrap=NONE, undo=True, maxundo=-1, autoseparators=True)
        scrollb = Scrollbar(value_label, command=self.value.yview)
        scrollb_h = Scrollbar(value_label, command=self.value.xview, orient=HORIZONTAL)
        self.value['yscrollcommand'] = scrollb.set
        self.value['xscrollcommand'] = scrollb_h.set
        self.value.grid(row=0, column=0, padx=2, pady=2, sticky=NSEW)
        value_label.grid(row=1, column=0, padx=5, sticky=NSEW)
        value_label.columnconfigure(0, weight=1)
        value_label.rowconfigure(0, weight=1)
        scrollb.grid(row=0, column=1, sticky=N+S)
        scrollb_h.grid(row=1, column=0, sticky=E+W)
        next_value = Button(self.value, text='>', padding='0 0 0 0', width=4, command=self.set_next_msg, cursor='arrow')
        next_value.place(relx=1.0, rely=1.0, x=-2, y=-2,anchor="se")
        prev_value = Button(self.value, text='<', padding='0 0 0 0', width=4, command=self.set_prev_msg, cursor='arrow')
        prev_value.place(relx=1.0, rely=1.0, x=-35, y=-2,anchor="se")
        json_button = Button(self.value, text='toJson', padding='0 0 0 0', width=6, command=self.format_json, cursor='arrow')
        json_button.place(relx=1.0, rely=1.0, x=-68, y=-2,anchor="se")
        xml_button = Button(self.value, text='toXml', padding='0 0 0 0', width=6, command=self.format_xml, cursor='arrow')
        xml_button.place(relx=1.0, rely=1.0, x=-113, y=-2,anchor="se")

        left_frame.grid(row=0, column=0, sticky=NSEW)
        left_frame.rowconfigure(0, weight=1)
        left_frame.rowconfigure(1, weight=11, pad=150)
        left_frame.columnconfigure(0, weight=1)

        outtxt_label = LabelFrame(window_frame, text='Log', padding='2 2 2 2')
        self.outtxt = Text(outtxt_label)
        scrollb = Scrollbar(outtxt_label, command=self.outtxt.yview)
        self.outtxt['yscrollcommand'] = scrollb.set
        self.outtxt.grid(row=0, column=0, sticky=NSEW)
        self.outtxt.tag_configure('odd', background='white', foreground='black')
        self.outtxt.tag_configure('even', background='grey', foreground='white')
        outtxt_label.grid(row=0, column=1, padx=5, rowspan=3, sticky=N+S+E+W)
        outtxt_label.rowconfigure(0, weight=1)
        outtxt_label.columnconfigure(0, weight=1)
        scrollb.grid(row=0, column=1, sticky='nsew')

        window_frame.grid(row=0, column=0, padx=0, sticky=N+W+E+S)
        window_frame.rowconfigure(0, weight=1)
        window_frame.columnconfigure(0, weight=1, pad=800)
        window_frame.columnconfigure(1, weight=1)
        tab.rowconfigure(0, weight=1)
        tab.columnconfigure(0, weight=1)
        self.update_lists()
        self.update_message()
    def send_to_kafka(self, servers, topic, key, value :str, outtxt: Text):
        tag = 'odd'
        if self.outtxt_index % 2 == 0:
            tag = 'even'
        self.outtxt_index += 1
        out_str = ''
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            if(str(value).startswith('0x')):
                value = value[2:]
                _value = binascii.unhexlify(value)
            else:
                _value = str(value).encode('utf-8')
            producer.send(topic, key=str(key).encode('utf-8'), value=_value)
            out_str = str(datetime.now()) + ': ' + 'Message sent' + '\n'
            producer.close()
        except NoBrokersAvailable as e:
            out_str = str(datetime.now()) + ': ' + str(e) + '\n'
        except KafkaTimeoutError as e:
            out_str = str(datetime.now()) + ': ' + str(e) + '\n'
        start_txt_index = float(outtxt.index(END)) - 1
        end_txt_index = str(floor(start_txt_index)) + '.' + str(len(out_str))
        outtxt.insert(END, out_str)
        outtxt.tag_add(tag, start_txt_index, end_txt_index)
        outtxt.see(END)
        
    def send(self, save=True):
        _broker = str(self.servers.get()).strip()
        _topic = str(self.topic.get()).strip()
        _key = str(self.key.get()).strip()
        _value = str(self.value.get('1.0', 'end-1c')).strip()
        if len(_broker) == 0 or len(_topic) == 0 or len(_value) == 0 or len(_key) == 0:
            messagebox.showwarning(title='Fields required',message='Broker, topic, key and message fields are required')
            return
        if save:
            with SqlliteConn(db_name=db_name) as conn:
                conn.execute('insert or replace into brokers(value) values(?)', (_broker,))
                conn.execute('insert or replace into topics(value) values(?)', (_topic,))
                conn.execute('insert or replace into keys(value) values(?)', (_key,))
                conn.execute('insert or replace into messages(value) values(?)', (_value,))
            relength_tables()
            self.update_lists()
            self.update_message()
        else:
            if self.message_timestamp is None:
                dt = datetime.now()
            else:
                dt = datetime.strptime(self.message_timestamp, '%Y-%m-%d %H:%M:%S')
            self.message_timestamp = (dt + timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
        if len(_broker)>0 and len(_topic)>0 and len(_key)>0 and len(_value)>0:
            self.send_to_kafka(_broker, _topic, _key, _value, self.outtxt)

    def update_lists(self):
        self.servers['values'] = get_list('brokers')
        if len(self.servers['values'])>0:
            self.servers.current(0)
        self.topic['values'] = get_list('topics')
        if len(self.topic['values'])>0:
            self.topic.current(0)
        self.key['values'] = get_list('keys')
        if len(self.key['values'])>0:
            self.key.current(0)

    def update_message(self):
        msg,ts = get_latest_msg()
        if ts is None:
            return
        self.value.delete('1.0', END)
        self.value.insert(END, msg)
        self.message_timestamp = ts

    def set_prev_msg(self):
        msg,ts = get_prev_msg(self.message_timestamp)
        if ts is not None:
            self.message_timestamp = ts
            self.value.delete('1.0', END)
            self.value.insert(END, msg)
    def format_json(self):
        try:
            _value = str(self.value.get('1.0', 'end-1c')).strip()
            if len(_value) == 0:
                return
            j = json.loads(_value)
            _value = json.dumps(j, indent=2)
            self.value.delete('1.0', END)
            self.value.insert(END, _value)
        except:
            tag = 'odd'
            if self.outtxt_index % 2 == 0:
                tag = 'even'
            self.outtxt_index += 1
            out_str = str(datetime.now()) + ': ' + str('Unable to format as json') + '\n'
            start_txt_index = float(self.outtxt.index(END)) - 1
            end_txt_index = str(floor(start_txt_index)) + '.' + str(len(out_str))
            self.outtxt.insert(END, out_str)
            self.outtxt.tag_add(tag, start_txt_index, end_txt_index)
            self.outtxt.see(END)
    def format_xml(self):
        try:
            _value = str(self.value.get('1.0', 'end-1c')).strip()
            if len(_value) == 0:
                return
            formatter = xmlformatter.Formatter(indent="1", indent_char="\t", encoding_output="UTF-8", preserve=["literal"])
            _value = formatter.format_string(_value)
            if _value:
                self.value.delete('1.0', END)
                self.value.insert(END, _value)
        except:
            tag = 'odd'
            if self.outtxt_index % 2 == 0:
                tag = 'even'
            self.outtxt_index += 1
            out_str = str(datetime.now()) + ': ' + str('Unable to format as xml') + '\n'
            start_txt_index = float(self.outtxt.index(END)) - 1
            end_txt_index = str(floor(start_txt_index)) + '.' + str(len(out_str))
            self.outtxt.insert(END, out_str)
            self.outtxt.tag_add(tag, start_txt_index, end_txt_index)
            self.outtxt.see(END)
    def set_next_msg(self):
        msg,ts = get_next_msg(self.message_timestamp)
        if ts is not None:
            self.message_timestamp = ts
            self.value.delete('1.0', END)
            self.value.insert(END, msg)

    def show_about(self):
        d = About(self.main_window)
        self.main_window.wait_window(d.tp)

def on_closing():
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            root.destroy()

class CustomNotebook2(Notebook):
    """A ttk Notebook with close buttons on each tab"""

    def __init__(self, *args, **kwargs):
        Notebook.__init__(self, *args, **kwargs)

        self._active = None

        self.bind("<ButtonPress-2>", self.on_close_press, True)
        self.bind("<ButtonRelease-2>", self.on_close_release)
    
    def on_close_press(self, event):
        """Called when the button is pressed over the close button"""
        index = self.index("@%d,%d" % (event.x, event.y))
        self.state(['pressed'])
        self._active = index
        return "break"

    def on_close_release(self, event):
        """Called when the button is released"""
        if not self.instate(['pressed']):
            return
        index = self.index("@%d,%d" % (event.x, event.y))
        if self._active == index:
            total = self.index('end')
            if total == 2 or index == total - 1:
                return
            if index  == total - 2:
                self.select(total - 3)
            active_object = self.nametowidget(self.tabs()[index])
            self.forget(index)
            active_object.destroy()
            self.event_generate("<<NotebookTabClosed>>")
        self.state(["!pressed"])
        self._active = None

def add_tab(tabControl: Notebook, root: Tk):
    tab = Frame(tabControl)
    last_tab = str(tabControl.index('end'))
    tabControl.add(tab, text = get_tab_name(last_tab))
    ProducerUI(tab, root)
    tab3 = Frame(tabControl)
    b = Button(tab3, command= lambda: add_tab(tabControl, root))
    b.grid(row=0, column=0)
    tabControl.add(tab3, text ='+')
    tabControl.forget(tabControl.select())

def tab_change(*args):
    t_nos=tabControl.index(tabControl.select())
    last_tab = tabControl.index('end') - 1
    if t_nos == last_tab:
        add_tab(tabControl, root)

def get_tab_name(idx: str):
    return idx + ' Producer'

init_db()
root = Tk()
root.title('Producer for Kafka')
root.protocol("WM_DELETE_WINDOW", on_closing)
root.style = Style()
root.geometry('1024x768')
root.minsize(width=800, height=768)
root.style.theme_use("vista")
customed_style = Style()
customed_style.configure('Custom.TNotebook.Tab', padding=[15, 5])
tabControl = CustomNotebook2(root, style="Custom.TNotebook")
tab1 = Frame(tabControl)
tab2 = Frame(tabControl)
Grid.columnconfigure(root, 0, weight=1)
Grid.rowconfigure(root, 0, weight=1)
last_tab = str(tabControl.index('end') + 1)
tabControl.add(tab1, text = get_tab_name(last_tab))
tabControl.add(tab2, text='+')
tabControl.bind('<<NotebookTabChanged>>', tab_change)
ProducerUI(tab1, root)
tabControl.grid(row=0, column=0, sticky='nsew')
root.mainloop()
