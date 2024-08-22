import json

path = "D:/Work/1387/RetailMoneyTransferDtcLegacyTransaction_1.01.003.json"
path_to_result = "D:/Work/1387/dummy.json"

dummy = {}

### Рекурсия. Что можем встретить: массив, anyOf, enum, ссылку на объект и сам объект
def get_data_into_ref(item, item_data, data):
    new_data = {}
    if item_data.get("type") == "object":
        for item in item_data.get("properties"):
            new_data[item] = get_data_into_ref(item, item_data.get("properties").get(item), data)
        return new_data
    if item_data.get("type") == "array":
        ### тут нужно вернуть массив вида [{"var1": 123, "var2": 123}]
        ### вытащить из $ref ссылку на сущность
        for item in data.get(item_data.get("items").get("$ref").split("/")[-1]).get("properties"):
            new_data[item] = get_data_into_ref(item, data.get(item_data.get("items").get("$ref").split("/")[-1]).get("properties").get(item), data)
        return [new_data]
    elif item_data.get("anyOf"):
        ###тут нужно дернуть любой элемент из anyOf и пройтись по нему
        for item in data.get(item_data["anyOf"][0].get("$ref").split("/")[-1]).get("properties"):
            new_data[item] = get_data_into_ref(item, data.get(item_data["anyOf"][0].get("$ref").split("/")[-1]).get("properties").get(item), data)
        return new_data
    elif item_data.get("enum"):
        ### если ссылка ведет на enum
        return item_data.get("enum")[0]
    elif item_data.get("$ref"):
        new_data = get_data_into_ref(item, data.get(item_data.get("$ref").split("/")[-1]), data)
        return new_data
    elif item_data.get("type") == "string":
        return item
    elif item_data.get("type") == "number" or item_data.get("type") == "integer":
        return 1234
    elif item_data.get("type") == "boolean":
        return True



with open(path, encoding="utf-8-sig") as file:
    data: dict = json.load(file)
    dummy = get_data_into_ref(dummy, data, data.get("definitions"))

with open(path_to_result, "w", encoding="utf-8") as file:
    json.dump(dummy, file, indent=4)
    print("Сохранили пример по пути: ", path_to_result)