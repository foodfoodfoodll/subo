from tkinter import *
from tkinter import messagebox, filedialog
from tkinter.ttk import Checkbutton
import parsing_json
from tkinter.scrolledtext import ScrolledText

def print_message(text, widget, output_to_file=False, path=''):
    widget.insert(END, str(text) + '\n')
    if output_to_file:
        with open(path+'/result.txt', 'a', encoding='utf-8') as file:
            file.write(text + '\n') 

def print():
    try:
        config = {
            "files_directory": files_directory.get(),
            "mappings_directory": mappings_directory.get(),
            "database_name": database_name.get(),
            "exclude_columns": exclude_columns.get().replace(' ', '').split(','),
            "continue_if_the_structure_does_not_match": continue_if_the_structure_does_not_match.get(),
            "output_only_failures": output_only_failures.get(),
            "output_examples": output_examples.get(),
            "output_in_file": output_in_file.get(),
            "output_file_path": files_directory.get() + "/result.txt"  
        }
        parsing_json.main(config)
        messagebox.showinfo(title="Информация", message="Готово! Результат записан в файл result.txt")
    except Exception as err:
        messagebox.showinfo(title="Ошибка", message=f"{err=}, {type(err)=}")
    except:
        messagebox.showinfo(title="Ошибка", message="Что-то пошло не так...")

def main(window, files_directory, mappings_directory, database_name, exclude_columns,
         continue_if_the_structure_does_not_match, output_only_failures, output_examples, output_in_file, result_widget):
    if files_directory == '':
        messagebox.showinfo(title="Ошибка", message="Выберите директорию с файлами!")
        return
    if mappings_directory == '':
        messagebox.showinfo(title="Ошибка", message="Выберите директорию с маппингами!")
        return
    if database_name == '':
        messagebox.showinfo(title="Ошибка", message="Заполните код базы данных!")
        return
    
    config = {
        "files_directory": files_directory,
        "mappings_directory": mappings_directory,
        "database_name": database_name,
        "exclude_columns": exclude_columns.replace(' ', '').split(','),
        "continue_if_the_structure_does_not_match": continue_if_the_structure_does_not_match,
        "output_only_failures": output_only_failures,
        "output_examples": output_examples,
        "output_in_file": output_in_file
    }
    parsing_json.main(config, result_widget)

window = Tk()
window.title("Проверятор")
window.geometry('600x380')

files_directory = StringVar()
mappings_directory = StringVar()
database_name = StringVar()
exclude_columns = StringVar()

files_directory.set('C:/Users/okachan/Documents/втб/файлы/test/SalesPoint')
mappings_directory.set('C:/Users/okachan/Documents/втб/файлы/test/SalesPoint')
database_name.set('batp')

continue_if_the_structure_does_not_match = BooleanVar()  
output_examples = BooleanVar()  
output_only_failures = BooleanVar()  
output_in_file = BooleanVar()  

files_directory_lbl = Label(window, text="Выберите директорию с файлами:")
files_directory_txt = Entry(window, width=54, textvariable=files_directory, bg='white')
files_directory_btn = Button(window, text="...", command=lambda: files_directory.set(filedialog.askdirectory()), bg='white', width=2)

files_directory_lbl.place(x=10, y=10) 
files_directory_txt.place(x=225, y=13)
files_directory_btn.place(x=555, y=8)


mappings_directory_lbl = Label(window, text="Выберите директирию с маппингами:")
mappings_directory_txt = Entry(window, width=54, textvariable=mappings_directory, bg='white')
mappings_directory_btn = Button(window, text="...", command=lambda: mappings_directory.set(filedialog.askdirectory()), bg='white', width=2)

mappings_directory_lbl.place(x=10, y=35) 
mappings_directory_txt.place(x=225, y=38)
mappings_directory_btn.place(x=555, y=33)


database_name_lbl = Label(window, text="Код базы данных:")
database_name_txt = Entry(window, width=76, textvariable=database_name, bg='white')

database_name_lbl.place(x=10, y=60) 
database_name_txt.place(x=120, y=63)

exclude_columns.set('changeid, changetype, changetimestamp, hdp_processed_dttm')
exclude_columns_lbl = Label(window, text="Технические поля:")
exclude_columns_txt = Entry(window, width=76, textvariable=exclude_columns, bg='white')

exclude_columns_lbl.place(x=10, y=85) 
exclude_columns_txt.place(x=120, y=88)

continue_if_the_structure_does_not_match_chk = Checkbutton(window,var=continue_if_the_structure_does_not_match, text = 'Сравнить данные, если структура таблиц не совпадает')
continue_if_the_structure_does_not_match_chk.place(x=10, y=110) 

output_examples_chk = Checkbutton(window, var=output_examples, text = 'Вывести примеры несовпадающих записей')
output_examples_chk.place(x=10, y=135) 

output_only_failures_chk = Checkbutton(window, var=output_only_failures, text = 'Вывести только таблицы, которые не прошли проверку')
output_only_failures_chk.place(x=10, y=160) 

output_in_file_chk = Checkbutton(window, var=output_in_file, text = 'Вывести результат в файл')
output_in_file_chk.place(x=10, y=185) 

result_lbl = Label(window, text='Результат:')
result_lbl.place(x=10, y=210) 

result_btn = Button(window, text="Старт", width=15, height=2,bg='white',
                    command=lambda: main(window, files_directory.get(),
                                          mappings_directory.get(), 
                                          database_name.get(), 
                                          exclude_columns.get(),
                                          continue_if_the_structure_does_not_match.get(), 
                                          output_only_failures.get(), 
                                          output_examples.get(), 
                                          output_in_file.get(),
                                          res_txt))
result_btn.place(x=463, y=183) 

res_txt = ScrolledText(window, width=70,  height=8)
res_txt.place(x=10, y=235) 

window.mainloop()