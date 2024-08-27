import sys
import tkinter as tk
from tkinter import filedialog, messagebox
import yaml
import typing
from mappings_schema_registry.mapping import create_mapping

if typing.TYPE_CHECKING:
    from app.app import App


class Config:
    pass

config = Config()

labels = [
    "file_dir",
    "subo_name",
    "id_ris",
    "mapping_version",
    "system_target",
    "loadType",
    "docs",
    "developer",
    "database",
    "topic"
]

def setup_ui(app: "App", config_path):

    def on_closing():
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            root.destroy()
            sys.exit(0)

    def submit():
        values: dict = {}
        for i in range(10):
            entry = entries[i]
            field_name: str = labels[i]
            value = entry.get()
            values[field_name]: str = value

        for key, value in values.items():
            setattr(config, key, value)

        app.config = config
        with open(config_path, "w", encoding="utf-8") as file:
            yaml.dump(values, file, allow_unicode=True, default_style="'")
        root.destroy()

    def browse():
        import os
        directory = os.path.dirname(filedialog.askopenfilename())
        entries[0].delete(0, tk.END)
        entries[0].insert(0, directory)
    
    def new_window():
        schema_path = tk.StringVar()
        new_window = tk.Toplevel(root)
        new_window.focus_set()
        new_window.grab_set()
        new_window.title("Создание маппинга")
        new_window.geometry("400x200")
        new_window.config(bg="#C8FDF1")

        schema_path_lbl = tk.Label(new_window, text="Путь к схемам:", font=("Arial", 12), bg="#C8FDF1")
        schema_path_txt = tk.Entry(new_window, width=40, textvariable=schema_path, bg='white')
        schema_path_btn = tk.Button(new_window, text="...", command=lambda: schema_path.set(filedialog.askdirectory()), bg='white', width=2)

        schema_path_lbl.place(x=0, y=35) 
        schema_path_txt.place(x=130, y=35)
        schema_path_btn.place(x=350, y=35)

        create_button = tk.Button(new_window, text="Create", command=lambda: create_mapping(schema_path.get()))
        create_button.place(x=200, y=60)

        close_button = tk.Button(new_window, text="Закрыть окно", command=new_window.destroy)
        close_button.place(x=200, y=85)

        

    root = tk.Tk()
    root.title("Autogen")
    window_width = 650
    window_height = 280
    root.geometry(f"{window_width}x{window_height}+{210}+{210}")
    root.resizable(False, False)
    root.config(bg="#C8FDF1")

    default_values: dict = {}
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            default_values = yaml.safe_load(file)
    except FileNotFoundError:
        print("File config.yml not found")

    len_config = len(default_values)
    entries: list = []
    for i in range(0, len_config):
        label = tk.Label(root, text=labels[i], font=("Arial", 12), bg="#C8FDF1")
        label.grid(row=i, column=0, stick="E", padx=10, sticky="wens")
        entry = tk.Entry(root, width=50)
        entry.grid(row=i, column=1, ipadx = 8, sticky="wens")
        field_name = labels[i]
        entry.insert(0, default_values.get(field_name, ""))
        entries.append(entry)

    # Button "Browse"
    button_browse = tk.Button(root, text="...", command=browse)
    button_browse.grid(row=0, column=4, sticky="w")

    # Button "Create"
    create_button = tk.Button(root, text="Create", command=submit)
    create_button.grid(row=19, columnspan=3)

    # Button "Schema registry"
    create_button = tk.Button(root, text="Schema registry", command=new_window)
    create_button.grid(row=4, column=5)

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()