from tkinter import PanedWindow as TkPanedWindow
from tkinter import messagebox
from tkinter import Text as TkText  # Rename Text import to avoid conflict
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
import base64
import sys
import struct

db_name = 'producer_config.db'
version = '5'

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
        conn.execute('DROP TABLE IF EXISTS messages;')
        conn.execute('CREATE TABLE IF NOT EXISTS messages2 (value TEXT NOT NULL, headers TEXT, key TEXT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, CONSTRAINT messages2_pk PRIMARY KEY (value, timestamp));')

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
        conn.execute('delete from messages2 where timestamp in (select b.timestamp from messages2 b left join (select timestamp from messages2 order by timestamp desc limit ' + str(maxlen) + ') b1 on b1.timestamp = b.timestamp where b1.timestamp is null);')

def get_latest_msg():
    with SqlliteConn(db_name=db_name) as conn:
        cursor = conn.execute('select value, headers, key, timestamp from messages2 order by timestamp desc limit 1')
        for r in cursor:
            return (r[0], r[1], r[2], r[3])
    return (None, None, None, None)

def get_prev_msg(timestamp):
    with SqlliteConn(db_name=db_name) as conn:
        cursor = conn.execute('select value, headers, key, timestamp from messages2 where timestamp < ? order by timestamp desc limit 1', (timestamp,))
        for r in cursor:
            return (r[0], r[1], r[2], r[3])
    return (None, None, None, None)

def get_next_msg(timestamp):
    with SqlliteConn(db_name=db_name) as conn:
        cursor = conn.execute('select value, headers, key, timestamp from messages2 where timestamp > ? order by timestamp asc limit 1', (timestamp,))
        for r in cursor:
            return (r[0], r[1], r[2], r[3])
    return (None, None, None, None)

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
    def base64_encoding_checked(self):
        if self.type_base64.get()==1:
            self.type_hex.set(0)
    def hex_encoding_checked(self):
        if self.type_hex.get()==1:
            self.type_base64.set(0)
    def extra_headers_checked(self):
        if self.type_headers.get() == 1:
            self.headers_frame.grid()
            self.content_paned.add(self.headers_frame)
            try:
                total_width = self.main_window.winfo_width()
                self.window_frame.update_idletasks()
                self.window_frame.panecget(self.left_frame, 'width')  # Check if pane exists
                self.window_frame.paneconfigure(self.left_frame, width=int(total_width * 0.75))
                self.content_paned.update_idletasks()
                self.content_paned.sashpos(0, int(total_width * 0.50))
            except:
                pass
        else:
            self.content_paned.forget(self.headers_frame)
            self.headers_frame.grid_remove()
            try:
                total_width = self.main_window.winfo_width()
                self.window_frame.update_idletasks()
                self.window_frame.paneconfigure(self.left_frame, width=int(total_width * 0.75))
            except:
                pass
    def __init__(self, tab, root) -> None:
        self.message_timestamp = None
        self.outtxt_index = 0
        self.main_window = root
        self._formatting_headers = False
        self.json_tags_configured = False

        # Create PanedWindow using tkinter's version, not ttk's
        self.window_frame = TkPanedWindow(tab, orient=HORIZONTAL, sashwidth=5, showhandle=True)
        self.window_frame.grid(row=0, column=0, sticky=NSEW)
        
        self.left_frame = Frame(self.window_frame)  # Store reference to left_frame
        buttons_frame = Frame(self.left_frame)
        
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
        
        checkboxes_frame = LabelFrame(buttons_frame, text='Message encoding', padding='2 2 2 2')
        self.type_base64 = IntVar()
        self.type_hex = IntVar()
        self.type_headers = IntVar()
        is_type_hex = Checkbutton(checkboxes_frame, text='Hex Encoded', variable=self.type_hex, command=self.hex_encoding_checked)
        is_type_base64 = Checkbutton(checkboxes_frame, text='Base64 Encoded', variable=self.type_base64, command=self.base64_encoding_checked)
        is_type_headers = Checkbutton(checkboxes_frame, text='Extra Headers', variable=self.type_headers, command=self.extra_headers_checked)
        is_type_hex.grid(row=0, column=0, sticky=S+W)
        is_type_base64.grid(row=0, column=1, sticky=S+W)
        is_type_headers.grid(row=0, column=2, sticky=S+W)
        checkboxes_frame.grid(row=3, column=0, padx=5, sticky=N+W+E)
        
        partition_label = Label(topic_label, text='Partition', padding='2 2 2 2')
        partition_label.grid(row=0, column=1, sticky=N+W+E)
        self.partition = Combobox(topic_label)
        self.partition.grid(row=0, column=2, sticky=N+W+E)
        self.partition['values'] = ['auto']
        self.partition.current(0)

        buttons_frame.grid(row=0, column=0, sticky=N+W+E+S)
        buttons_frame.columnconfigure(0, weight=1)

        # Create content area with PanedWindow for message and headers
        self.content_paned = TkPanedWindow(self.left_frame, orient=HORIZONTAL)  # Make it instance variable
        self.content_paned.grid(row=1, column=0, sticky=NSEW)
        
        # Message area
        value_label = LabelFrame(self.content_paned, text='Message', padding='2 2 2 2')
        self.value = TkText(value_label, wrap=NONE, undo=True, maxundo=-1, autoseparators=True)  # Use TkText instead
        scrollb = Scrollbar(value_label, command=self.value.yview)
        scrollb_h = Scrollbar(value_label, command=self.value.xview, orient=HORIZONTAL)
        self.value['yscrollcommand'] = scrollb.set
        self.value['xscrollcommand'] = scrollb_h.set
        next_value = Button(self.value, text='>', padding='0 0 0 0', width=4, command=self.set_next_msg, cursor='arrow')
        next_value.place(relx=1.0, rely=1.0, x=-2, y=-2,anchor="se")
        prev_value = Button(self.value, text='<', padding='0 0 0 0', width=4, command=self.set_prev_msg, cursor='arrow')
        prev_value.place(relx=1.0, rely=1.0, x=-35, y=-2,anchor="se")
        json_button = Button(self.value, text='toJson', padding='0 0 0 0', width=6, command=self.format_json, cursor='arrow')
        json_button.place(relx=1.0, rely=1.0, x=-68, y=-2,anchor="se")
        xml_button = Button(self.value, text='toXml', padding='0 0 0 0', width=6, command=self.format_xml, cursor='arrow')
        xml_button.place(relx=1.0, rely=1.0, x=-113, y=-2,anchor="se")
        self.value.grid(row=0, column=0, sticky=NSEW)
        scrollb.grid(row=0, column=1, sticky=NS)
        scrollb_h.grid(row=1, column=0, sticky=EW)
        value_label.columnconfigure(0, weight=1)
        value_label.rowconfigure(0, weight=1)
        self.content_paned.add(value_label)

        # Headers area - create but don't add to paned window initially
        self.headers_frame = LabelFrame(self.content_paned, text='Headers (single level JSON)', padding='2 2 2 2')
        self.headers = TkText(self.headers_frame, wrap=NONE, undo=True, maxundo=-1, autoseparators=True)  # Use TkText instead
        self.headers.bind('<FocusOut>', self.on_headers_focus_out)
        headers_scrollb = Scrollbar(self.headers_frame, command=self.headers.yview)
        headers_scrollb_h = Scrollbar(self.headers_frame, command=self.headers.xview, orient=HORIZONTAL)
        self.headers['yscrollcommand'] = headers_scrollb.set
        self.headers['xscrollcommand'] = headers_scrollb_h.set
        self.headers.grid(row=0, column=0, sticky=NSEW)
        headers_scrollb.grid(row=0, column=1, sticky=NS)
        headers_scrollb_h.grid(row=1, column=0, sticky=EW)
        self.headers_frame.columnconfigure(0, weight=1)
        self.headers_frame.rowconfigure(0, weight=1)
        self.headers_frame.grid_remove()  # Initially hidden

        # Configure left frame layout
        self.left_frame.rowconfigure(0, weight=0)  # Buttons area
        self.left_frame.rowconfigure(1, weight=1)  # Content area
        self.left_frame.columnconfigure(0, weight=1)
        
        # Log area
        outtxt_label = LabelFrame(self.window_frame, text='Log', padding='2 2 2 2')
        self.outtxt = TkText(outtxt_label)
        scrollb = Scrollbar(outtxt_label, command=self.outtxt.yview)
        self.outtxt['yscrollcommand'] = scrollb.set
        self.outtxt.grid(row=0, column=0, sticky=NSEW)
        scrollb.grid(row=0, column=1, sticky=NS)
        outtxt_label.columnconfigure(0, weight=1)
        outtxt_label.rowconfigure(0, weight=1)

        # Add main sections to window PanedWindow with proper weights
        self.window_frame.add(self.left_frame)
        self.window_frame.add(outtxt_label)

        # Configure tab layout
        tab.rowconfigure(0, weight=1)
        tab.columnconfigure(0, weight=1)

        self.update_lists()
        self.update_message()

        # Set initial sash position for proper proportions
        self.main_window.update_idletasks()
        # Position sash to give left frame 75% and log 25% of width
        total_width = self.main_window.winfo_width()
        self.window_frame.update_idletasks()
        self.window_frame.paneconfigure(self.left_frame, width=int(total_width * 0.75))

        # Set minimum sizes
        self.left_frame.configure(width=600)  # Minimum width for left frame
        outtxt_label.configure(width=200)  # Minimum width for log
    
    def to_bytes_array(self, data):
        if isinstance(data, bytes):
            return data
        elif isinstance(data, str):
            return data.encode('utf-8')
        elif isinstance(data, int):
            return data.to_bytes((data.bit_length() + 7) // 8, byteorder='big')
        elif isinstance(data, float):
            return struct.pack('!d', data)  # Use struct for consistent float representation
        elif isinstance(data, bool):
            return struct.pack('!?', data)
        elif data is None:
            return b''  # Represent None as an empty bytes array
        else:
            return None
    
    def send_to_kafka(self, servers, topic, partition, key, value :str, headers: str, outtxt: TkText):
        tag = 'odd'
        if self.outtxt_index % 2 == 0:
            tag = 'even'
        self.outtxt_index += 1
        out_str = ''
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            if self.type_hex.get() == 1:
                _value = binascii.unhexlify(value)
            elif self.type_base64.get() == 1:
                _value = base64.b64decode(value)
            else:
                _value = str(value).encode('utf-8')
            _headers = json.loads(headers) if headers else None
            _send_headers = []
            if _headers is not None:
                for k, v in _headers.items():
                    val = self.to_bytes_array(v)
                    if isinstance(val, bytes):
                        _send_headers.append((k, val))
            result = producer.send(topic, key=str(key).encode('utf-8'), value=_value, headers=_send_headers, partition=partition)
            sent_partition = str(result.get().partition)
            out_str = str(datetime.now()) + ': ' + 'Message sent to partition: ' + sent_partition + '\n'
            producer.close()
        except (NoBrokersAvailable, KafkaTimeoutError, json.JSONDecodeError) as e:
            out_str = str(datetime.now()) + ': ' + str(e) + '\n'
        except:
            out_str = str(datetime.now()) + ': ' + str(sys.exc_info()[1]) + '\n'
        start_txt_index = float(outtxt.index(END)) - 1
        end_txt_index = str(floor(start_txt_index)) + '.' + str(len(out_str))
        outtxt.insert(END, out_str)
        outtxt.tag_add(tag, start_txt_index, end_txt_index)
        outtxt.see(END)
    
    def num(self, s):
        try:
            return int(s)
        except ValueError:
            return None
    
    def sanitize_partition(self, partition: str):
        if partition == 'auto':
            return None
        partition_int = self.num(partition)
        if partition_int is None or partition_int < 0:
            return None
        return partition_int
    
    def send(self, save=True):
        _broker = str(self.servers.get()).strip()
        _topic = str(self.topic.get()).strip()
        _key = str(self.key.get()).strip()
        _value = str(self.value.get('1.0', 'end-1c')).strip()
        _partition = str(self.partition.get()).strip()
        _headers = ""
        if self.type_headers.get() == 1:  # Only get headers if checkbox is checked
            _headers = str(self.headers.get('1.0', 'end-1c')).strip()
            if len(_headers) > 0:
                try:
                    json.loads(_headers)  # Validate JSON
                except json.JSONDecodeError:
                    messagebox.showerror(title='Invalid Headers', message='Headers must be valid JSON.')
                    return
        if len(_broker) == 0 or len(_topic) == 0 or len(_value) == 0 or len(_key) == 0:
            messagebox.showwarning(title='Fields required',message='Broker, topic, key and message fields are required')
            return
        if save:
            with SqlliteConn(db_name=db_name) as conn:
                conn.execute('insert or replace into brokers(value) values(?)', (_broker,))
                conn.execute('insert or replace into topics(value) values(?)', (_topic,))
                conn.execute('insert or replace into keys(value) values(?)', (_key,))
                conn.execute('insert or replace into messages2(value, headers, key) values(?, ?, ?)', (_value, _headers, _key))
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
            self.send_to_kafka(_broker, _topic, self.sanitize_partition(_partition), _key, _value, _headers, self.outtxt)

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
        msg, headers, key, ts = get_latest_msg()
        if ts is None:
            return
        self.value.delete('1.0', END)
        self.value.insert(END, msg)
        self.headers.delete('1.0', END)
        if headers:
            self.headers.insert(END, headers)
        self.key.set(key)
        self.message_timestamp = ts

    def set_prev_msg(self):
        msg, headers, key, ts = get_prev_msg(self.message_timestamp)
        if ts is not None:
            self.message_timestamp = ts
            self.value.delete('1.0', END)
            self.value.insert(END, msg)
            self.headers.delete('1.0', END)
            if headers:
                self.headers.insert(END, headers)
            self.key.set(key)
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
        msg, headers, key, ts = get_next_msg(self.message_timestamp)
        if ts is not None:
            self.message_timestamp = ts
            self.value.delete('1.0', END)
            self.value.insert(END, msg)
            self.headers.delete('1.0', END)
            if headers:
                self.headers.insert(END, headers)
            self.key.set(key)

    def show_about(self):
        d = About(self.main_window)
        self.main_window.wait_window(d.tp)

    def format_headers_json(self, event=None):
        try:
            content = str(self.headers.get('1.0', 'end-1c')).strip()
            if len(content) > 0:
                try:
                    parsed = json.loads(content)
                    formatted = json.dumps(parsed, indent=4)
                    if formatted != content:  # Only update if content changed
                        self.headers.delete('1.0', END)
                        self.headers.insert(END, formatted)
                except json.JSONDecodeError:
                    pass  # Only format when content is valid JSON
        finally:
            self._formatting_headers = False

    def on_headers_focus_out(self, event=None):
        self.format_headers_json()

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
