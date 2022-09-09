Задание 1
Для каждого значения скорости ПК, превышающего 600 МГц, определите среднюю цену ПК с такой же скоростью.
Вывести: speed, средняя цена.

SELECT pc.speed, AVG(pc.price)
FROM pc
WHERE pc.speed > 600
GROUP BY pc.speed

Задание 2
Вывести все строки из таблицы Product, кроме трех строк с наименьшими номерами моделей и трех строк с наибольшими номерами моделей.
  Select maker, model, type from
  (
  Select
  row_number() over (order by model) p1,
  row_number() over (order by model DESC) p2,
  from Product
  ) t1
  where p1 > 3 and p2 > 3z
  
  Задание 3
  Найти тех производителей ПК, все модели ПК которых имеются в таблице PC.
  SELECT p.maker
  FROM product p
  LEFT JOIN pc ON pc.model = p.model
  WHERE p.type = 'PC'
  GROUP BY p.maker
  HAVING COUNT(p.model) = COUNT(pc.model)
  
  Задание 4
Найдите производителей принтеров,
которые производят ПК с наименьшим объемом RAM и с самым быстрым процессором среди всех ПК,
имеющих наименьший объем RAM. Вывести: Maker
  SELECT DISTINCT maker                                                       5)
  FROM product
  WHERE model IN (
                  SELECT model                                                3)
                  FROM pc
                  WHERE ram = (
                               SELECT MIN(ram)                                1)
                               FROM pc
                              )
                  AND speed = (
                               SELECT MAX(speed)                              2)
                               FROM pc
                               WHERE ram = (
                                            SELECT MIN(ram)                   1)
                                            FROM pc
                                           )
                              )
                 )
  AND maker IN (
                SELECT maker                                                  4)
                FROM product
                WHERE type='printer'
               )
               
