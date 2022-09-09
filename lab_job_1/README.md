Lab1
Загрузка таблицы, Создать объект SparkSession.
1. Создать неявный объект SparkSession, используя метод builder()
2. Установить master в local[*]
3.Установить имя приложения spark - sql - labs
4.Задать следующие параметы для DataFrameReader
.format("csv")
.option("nullValue", "NULL")
.option("delimiter", "\t")
.schema(схема_таблицы)
.load(путь)
.createTempView("customer")
5.Выполнить селект используя SQL и вывести результат

Lab2
Пример считывания таблиц
Создать объект SparkSession
Загрузить таблицу используя метод table/sql
1. Создать неявный объект SparkSession, используя метод builder()
2. Установить master в local[*]
3. Установить имя приложения spark-sql-labs
6. Загрузить таблицу customer, используя метод table()
7. Загрузить таблицу product, используя метод sql() и в качестве аргумента запрос: "FROM product"
8. Загрузить таблицу order, используя метод sql и в качестве аргумента запрос:
"SELECT DISTINCT customer_id, order_date FROM order WHERE status = 'delivered'"
9. Вывести таблицы используя метод show()
в качестве аргумента можно указать лимит в кол-во строк и тип выводимой строки полный/урезанный

Lab3
Пример обработки полей при помощи select/withColumns
Вывести название всех устройств,
если цена больее 50000 вычесть 10% от стоимости назвать поле new_price,
добавить поле type, используя функцию getTypeDevice
Итоговое множество содержит поля: product.name, new_price, type
1. Создать неявный объект SparkSession, используя метод builder()
2. Установить master в local[*]
3. Установить имя приложения spark-sql-labs
6. Загрузить таблицу product в DataFrame
7. В import udf добавить так же when и col из того же пакета
8. В методе select() выбрать поле name, используя функцию when().otherwise()
расчитать новую стоимость девайса
9. Используя метод withColumn и функцию getTypeDevice, добавить к выборке поле type
10. Вывести результат используя метод show() или записать DataFrame в файл

Lab4
Пример использования filter, join, crossJoin, orderBy
Вывести информацию о клиенте email, название продукта
и кол-во доставленного товара за первую половину 2018 года
Итоговое множество содержит поля: customer.email, product.name, order.order_date, order.number_of_product
1. Создать неявный объект SparkSession, используя метод builder()
2. Установить master в local[*]
3. Установить имя приложения spark-sql-labs
6. Загрузить в DataFrame таблицу order
7. Загрузить в DataFrame таблицу customer, выбрать поля:
id далее customer_id, email
8. Загрузить в DataFrame таблицу product, выбрать поля:
id далее product_id, name далее product_name
9. Выполнить перекрестное соединение DataFrame из п.7 и п.8
11. Выполнить внутреннее соединение по полю customer_id, product_id
10. Выбрать транзакции co статусом "delivered" и датой заказа с 2018-01-01 по 2018-06-30
12. Выбрать поля email, product_name, order_date, number_of_products
13. Выполнить сортировку по полю email, order_date
14. Вывести результат используя метод show() или записать DataFrame в файл

Lab5
Пример использования join, groupBy, agg
Необходимо расчитать для каждого клиента, стоимость общей закупки каждого товара,
максимальный объем заказанного товара, минимальную стоимость заказа,
среднюю стоимость заказа за первую половину 2018 года, заказ должен быть доставлен
Итоговое множество содержит поля: customer.name, product.name, sum(order.number_of_product * price),
max(order.number_of_product), min(order.number_of_product * price), avg(order.number_of_product * price)
1. Создать неявный объект SparkSession, используя метод builder()
2. Установить master в local[*]
3. Установить имя приложения spark-sql-labs
6. Загрузить в DataFrame таблицу order
7. Выбрать транзакции со статусом delivered и датой заказа с 2018-01-01 по 2018-06-30
8. Загрузить в DataFrame таблицу customer, выбрать поля:
id далее customer_id, email
9. Загрузить в DataFrame таблицу product, выбрать поля:
id далее product_id, name далее product_name
10. Выполнить перекрестное соединение DataFrame из п.8 и п.9
11. Выполнить внутреннее соединение DataFrame из п.10 и п.7
12. Выполнить группировку по полям customer_name, product_name
13. Расчитать сумму по стоимости заказа, максимальный объем заказ, минимальную сумму заказа, среднюю сумму заказа.
14. Вывести результат используя метод show() или записать DataFrame в файл

Lab6
Пример использования оконных выражений
Необходимо определить самый популярный продукт у клиента
Итоговое множество содержит поля: customer.name, product.name
1. Создать неявный объект SparkSession, используя метод builder()
2. Установить master в local[*]
3. Установить имя приложения spark-sql-labs
6. Загрузить таблицу product в DataFrame
7. Выбрать поля: id далее product_id, name далее product_name
8. Загрузить таблицу customer в DataFrame
9. Выбрать поля: id далее customer_id, name далее customer_name
10. Выполнить перекрестное соединение DataFrame из п.7 и п.9
11. Загрузить таблицу order в DataFrame
12. Выполнить группировку по полю customer_id, product_id
13. Расчитать сумму по полю number_of_product, далее sum_num_of_product
14. Добавить import org.apache.spark.sql.expressions.Window
15. Написать оконную функцию с партицированием по полю customer_id
и сортирвокой в порядке убывания по полю sum_num_of_product
16. Использую аналитичексую функцию row_number и оконное выражение из п.15
добавить поле rn
17. Выбрать только те строки, в которых значение поля rn = 1
18. Выполнить внутреннее соединение DataFrame из п.17 и п.10 по полям:
customer_id, product_id
19. Выбрать поля customer_name, product_name
20. Вывести результат используя метод show() или записать DataFrame в файл
