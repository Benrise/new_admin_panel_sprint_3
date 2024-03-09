# ====================== Elastic Dev Tools Console Commands  ======================

# === Стандартный набор команд ===

# Показать все доступные команды CAT API
GET _cat

# Информация по шардам
GET _cat/shards

# Информация по индексам
GET _cat/indices

# Получить первые документы в индексе
GET test_index/_search

# Создать индекс
PUT test_index

# Удалить индекс
DELETE test_index

# Информация по определенному индексу
GET test_index

# Создать индекс с определённым количеством шардов
PUT test_index 
{
  "settings": {
    "number_of_shards": 5
  }
}

# Добавить документ в индекс
POST test_index/_doc
{
  "test_field": "test_value"
}

# Добавить новое поле в документ с указанным id
POST test_index/_update/w-XNHY4B7f5BFrLWneTM
{
  "doc": {
    "new_test_field": "new_test_value"
  }
}

# Получить документа по указанному id
GET test_index/_doc/w-XNHY4B7f5BFrLWneTM

# Узнать, на какой шард улетел (улетит) документ с указанным id
GET test_index/_search_shards?routing=w-XNHY4B7f5BFrLWneTM

# Удалить документ с указанным id
DELETE test_index/_doc/xOW3Ho4B7f5BFrLWQuQQ

# === Работа с анализатором ===

# Анализ текста, по умолчанию - стандартный анализатор

POST /_analyze
{"text": "Мама, мыла раму!", "analyzer": "standard"}

# Кастомный анализатор
POST /_analyze
{
  "text": "С днём рождения!",
  "char_filter": [
    {
     "type": "mapping",
     "mappings": [
       "c => ",
       "k => ",
       "С => ",
       "K => "
      ]
    }
  ],
  "tokenizer": "standard",
  "filter": ["uppercase"]
}

# === Расширенная работа с индексом ===

# Получить информацию по маппингу индекса
GET test_index/_mapping

# Создание индекса с динамическим маппингом

PUT test_index 

# Создание индекса со кастомным маппингом

PUT test_index
{
  "mappings": {
    "properties": {
      "test_field1": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "test_field2": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "test_field3": {
        "type": "integer"
      }
    }
  }
}

# Попытка положить в integer text

POST test_index/_doc
{
  "test_field1": "test_value1",
  "test_field2": "test_value2",
  "test_field3": "wrong_test_value3"
}

# === Работа с типами данных ===

# Создание индекса с кастомным маппингом и отключенным приведением типа для integer поля
PUT test_index
{
  "mappings": {
    "properties": {
      "name": {
        "type": "text"
      },
      "surname": {
        "type": "text"
      },
      "city": {
        "type": "text"
      },
      "rating": {
        "type": "integer",
        "coerce": false
      }
    }
  }
}

# Не сможем добавить документ с некорректным типом, т.к. автоприведение отключено

POST test_index/_doc
{
  "name": "Artemida",
  "surname": "Rise",
  "city": "Moscow",
  "rating": 11
}

# Работа с массивом

PUT products

DELETE products

POST products/_doc
{
  "tags": "simple tag"
}

POST products/_doc
{
  "tags": ["baby tag1", "tag2", "tag3"]
}

GET products/_search

# Работа с числовым массивом отличается

POST products/_doc
{
  "numbers": [1,2,3,4,5,6,7]
}

GET products/_search
GET products/_mapping

# Правило - до определения маппинга входные данные в коллекциях должны быть одного типа
# То есть кидать неоднородные данные до определения маппинга нельзя
# Error
POST products/_doc
{
  "numbers": [1,2,3,4,"5",6,7]
}
# Error
POST products/_doc
{
  "product_info": [
    {"name": "Nike", "rating": 5},
    {"name": "Adidas", "rating": "5"}
  ]
}

# Сначала однородные данные, определение маппинга, потом уже неоднородные
POST products/_doc
{
  "product_info": [
    {"name": "Nike", "rating": "5"},
    {"name": "Adidas", "rating": "5"}
  ]
}
POST products/_doc
{
  "product_info": [
    {"name": "NB", "rating": 3},
    {"name": "Monclear", "rating": "4"}
  ]
}




# Успешно добавили тэги, но в ES нет такого типа данных, как массив.
GET products/_mapping

# === Основы поиска ===

# Для поиска по текстовым полям используем match

GET test_index/_search
{
  "query": {
    "match": {
      "name": {
        "query": "Artemida"
      }
    }
  }
}

# Пересоздадим индекс, для каждого поля установим тип keyword

PUT test_index
{
  "mappings": {
    "properties": {
      "name": {
        "type": "keyword"
      },
      "surname": {
        "type": "keyword"
      },
      "city": {
        "type": "keyword"
       },
      "rating": {
        "type": "integer",
        "coerce": false
      }
    }
  }
}

# Для поиска по keyword полям используем term - потому что одно и то же поле имеет мультитип (является и text и keyword)

GET test_index/_search
{
  "query": {
    "term": {
      "name.keyword": {
        "value": "Artemida"
      }
    }
  }
}

# Маппинги вложенных структур

PUT users
POST users/_mapping
{
  "properties": {
    "name": {
      "type": "text"
    },
    "surname": {
      "type": "text"
    },
    "age": {
      "type": "integer"
    },
    "job_desc": {
      "properties": {
        "company_name": {
          "type": "text"
        },
        "position": {
          "type": "text"
        }
      }
    }
  }
}

# Альтернативная запись
POST users/_mapping
{
  "properties": {
    "name": {
      "type": "text"
    },
    "surname": {
      "type": "text"
    },
    "age": {
      "type": "integer"
    },
    "job_desc.company_name": {
      "type": "text"
    },
    "job_desc.position": {
      "type": "text"
    }
  }
}

GET users/_mapping

POST users/_doc 
{
  "name": "Ivan",
  "surname": "Ivanov",
  "age": 35,
  "job_desc.company_name": "Google",
  "job_desc.position": "Manager"
}

# Маппинг не изменится
POST users/_doc 
{
  "name": "Ivan",
  "surname": "Ivanov",
  "age": 35,
  "job_desc": {
    "company_name": "ARRIVAL",
    "position": "developer"
  }
}

# === Работа с датами ===

DELETE users

PUT users
{
  "mappings": {
    "properties": {
      "name": {
        "type": "text"
      },
      "surname": {
        "type": "text"
      },
      "age": {
        "type": "integer"
      },
      "job_desc": {
        "properties": {
          "company_name": {
            "type": "text"
          },
          "position": {
            "type": "text"
          },
          "work_since": {
            "type": "date"
          }
        }
      }
    }
  }
}

GET users/_mapping
GET users/_search

# Будет ошибка из-за некорректного формата даты
POST users/_doc 
{
  "name": "Ivan",
  "surname": "Ivanov",
  "age": 31,
  "job_desc": {
    "company_name": "ARRIVAL",
    "position": "developer",
    "work_since": "04/04/2022"
  }
}

# Корректный формат даты
POST users/_doc 
{
  "name": "Ivan",
  "surname": "Ivanov",
  "age": 31,
  "job_desc": {
    "company_name": "ARRIVAL",
    "position": "developer",
    "work_since": "2022-04-04"
  }
}

# Альтернативная запись с временной зоной Z (UTC)
POST users/_doc 
{
  "name": "Artem",
  "surname": "Ivanov",
  "age": 31,
  "job_desc": {
    "company_name": "Google",
    "position": "developer",
    "work_since": "2021-01-01T13:07:41Z"
  }
}

# Можно определять кастомный формат даты
PUT users
{
  "mappings": {
    "properties": {
      "name": {
        "type": "text"
      },
      "surname": {
        "type": "text"
      },
      "age": {
        "type": "integer"
      },
      "job_desc": {
        "properties": {
          "company_name": {
            "type": "text"
          },
          "position": {
            "type": "text"
          },
          "work_since": {
            "type": "date",
            "format": "dd/mm/YYYY"
          }
        }
      }
    }
  }
}

# Теперь ошибки не будет
POST users/_doc 
{
  "name": "Ivan",
  "surname": "Ivanov",
  "age": 31,
  "job_desc": {
    "company_name": "ARRIVAL",
    "position": "developer",
    "work_since": "04/04/2022"
  }
}

# Поисковой запрос: вывести сотрудников, которые работрают с 1 января 2022 года или позже
GET users/_search
{
  "query": {
    "range": {
      "job_desc.work_since": {
        "gte": "2022-01-01"
      }
    }
  }
}

# Поисковой запрос: вывести сотрудников, которые работрают с 1 января 2022 года, точное совпадение
GET users/_search
{
  "query": {
    "term": {
      "job_desc.work_since": {
        "value": "2022-01-01"
      }
    }
  }
}







