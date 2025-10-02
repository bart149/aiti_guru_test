from sqlalchemy import (Column, Integer, String,
                        create_engine, Float, ForeignKey, Table)
from sqlalchemy.orm import (declarative_base, relationship)

engine = create_engine('sqlite:///db_test.db', echo=True)
Base = declarative_base()

class Nomenclature(Base):
    '''
    Таблица Номенклатура
    name - наименование, не допускаю пустые значения
    count - количество на складе
    price - цена за ед. товара
    catalog_id - id из таблицы catalog
    category - добавил связь с полем products таблицы catalog
    orders - добавил связь с полем items таблицы order для связи с товарами (многие ко многим)
    '''
    __tablename__ = 'nomenclature'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    count = Column(Integer)
    price = Column(Float)
    catalog_id = Column(Integer, ForeignKey("catalog.id"))
    category = relationship("Catalog", back_populates="products")
    orders = relationship("Order", secondary="order_items", back_populates="items")

class Catalog(Base):
    '''
    Таблица Каталог товаров
    name - название категории
    parent_id - внешний ключ на id родительской категории 
    children - список дочерних категорий
    parent - название родительской категории
    products - список товаров относящийся к этой категории
    '''
    __tablename__ = 'catalog'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    # для добавления категорий любого уровня вложенности 
    # поле parent_id ссылаю на id этой же таблицы
    parent_id = Column(Integer, ForeignKey('catalog.id'), nullable=True)
    children = relationship("Catalog", back_populates="parent", remote_side=[id])
    parent = relationship("Catalog", back_populates="children", remote_side=[id])
    products = relationship("Nomenclature", back_populates="category")

class Client(Base):
    '''
    Таблица Клиенты
    name     - наименование клиента
    address  - адрес клиента 
    orders   - список заказов, связанных с этим клиентом
    '''
    __tablename__ = 'client'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    address = Column(String, nullable=True)
    orders = relationship("Order", back_populates="client")

''' Для реализации возможности делать заказ из разных наборов товаров
создал промежуточную таблицу order_items'''
order_items = Table(
    'order_items', Base.metadata,
    Column('order_id', Integer, ForeignKey('order.id'), primary_key=True),
    Column('nomenclature_id', Integer, ForeignKey('nomenclature.id'), primary_key=True),
    Column('quantity', Integer, nullable=False, default=1)
)

class Order(Base):
    '''
    Таблица Заказы покупателей
    client        - клиент которому принадлежит заказ
    client_id     - внешний ключ на клиента
    items         - список товаров, включённых в заказ
    '''
    __tablename__ = 'order'
    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey('client.id'))
    client = relationship("Client", back_populates="orders")
    items = relationship("Nomenclature", secondary=order_items, back_populates="orders")

# создаю базу данных
Base.metadata.create_all(engine)



    
#2.1
'''
Получение информации о сумме товаров
заказанных под каждого клиента 
(Наименование клиента, сумма)

SELECT c.name AS client_name,
SUM(n.price * oi.quantity) AS total_sum
FROM client c
LEFT JOIN "order" AS o 
ON o.client_id = c.id
LEFT JOIN order_items AS oi
ON oi.order_id = o.id
LEFT JOIN nomenclature AS n
ON n.id = oi.nomenclature_id
GROUP BY c.id, c.name
'''

#2.2
'''
Найти количество дочерних элементов
первого уровня вложенности для категорий номенклатуры.

SELECT parent.id AS category_id,
parent.name AS category_name,
COUNT(child.id) AS child_count
FROM catalog parent
LEFT JOIN catalog child ON child.parent_id = parent.id
GROUP BY parent.id, parent.name
'''

#2.3.1.
'''
Написать текст запроса для отчета (view) «Топ-5 
самых покупаемых товаров за последний месяц» 
(по количеству штук в заказах). В отчете должны быть: 
Наименование товара, Категория 1-го уровня, 
Общее количество проданных штук.

SELECT n.name AS product_name, c_root.name AS top_category_name,
SUM(oi.quantity) AS total_sold
FROM nomenclature n
INNER JOIN order_items oi ON oi.nomenclature_id = n.id
INNER JOIN order o ON o.id = oi.order_id AND o.date >= date('now','-1 month')
INNER JOIN catalog c ON c.id = n.catalog_id
LEFT JOIN catalog c_root ON c_root.id = COALESCE(c.parent_id, c.id)
GROUP BY n.id, n.name, c_root.name
ORDER BY total_sold DESC
LIMIT 5
'''


#2.3.2. Оптимизация
'''
Получилось так, что запрос требует соединения 4 таблиц,
что может тормозить при большом количестве данных.
А так же отсутствуют индексы на полях для содединений
order_items.order_id, order_items.nomenclature_id, order.date
nomenclature.catalog_id, catalog.parent_id
1. Можно задать индексы на указанных полях
2. Добавить поле top_category_id в таблицу catalog или nomenclature,
чтобы не вычислять категорию 1-го уровня и не делать join с родителем,
а сразу хранить её.
'''


#3
from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
from models import Base, Order, Nomenclature, Client

DATABASE_URL = "sqlite:///./aiti_guru.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI()


@app.post("/orders/{order_id}/add_item/")
def add_item_to_order(order_id: int, nomenclature_id: int, quantity: int):
    session = SessionLocal()
    try:
        # Получаем товар
        product = session.get(Nomenclature, nomenclature_id)
        if not product:
            raise HTTPException(status_code=404, detail="Товар не найден")
        if product.count < quantity:
            raise HTTPException(status_code=400, detail="Недостаточно товара на складе")

        # Получаем заказ
        order = session.get(Order, order_id)
        if not order:
            raise HTTPException(status_code=404, detail="Заказ не найден")

        # Проверяем, есть ли товар в заказе
        existing_item = None
        for item in order.items:
            if item.id == nomenclature_id:
                existing_item = item
                break

        if existing_item:
            # Увеличиваем количество через association proxy
            link = session.execute(
                order_items.select().where(
                    (order_items.c.order_id == order.id) &
                    (order_items.c.nomenclature_id == existing_item.id)
                )
            ).first()
            # Обновляем количество
            session.execute(
                order_items.update()
                .where(
                    (order_items.c.order_id == order.id) &
                    (order_items.c.nomenclature_id == existing_item.id)
                )
                .values(quantity=link.quantity + quantity)
            )
        else:
            # Добавляем товар 
            order.items.append(product)
            # Устанавливаем количество
            session.execute(
                order_items.update()
                .where(
                    (order_items.c.order_id == order.id) &
                    (order_items.c.nomenclature_id == product.id)
                )
                .values(quantity=quantity)
            )

        # Уменьшаем складской остаток
        product.count -= quantity

        session.commit()
        return {"message": "Товар добавлен в заказ"}
    finally:
        session.close()
