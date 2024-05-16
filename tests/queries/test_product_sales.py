from shared.queries import ProductSales
from shared.query import Query


class QueryTester:

    def __init__(self, query: Query, expected_sql: str) -> None:
        self.query = query
        self.expected_sql = expected_sql

    def test(self):
        self.test_create_sql()

    def test_create_sql(self):

        sql = self.query.create_sql()
        assert sql == self.expected_sql


def test_product_sales():

    query = ProductSales(sellout_id=85, product_ids=[1, 2], store_ids=[3, 4])
    expected_sql = """
    SELECT "sales"."product_id","sales"."sale_date" "date",
    SUM(COALESCE("sales"."sales",0)) "units",
    SUM(COALESCE("sales"."money",0)) "money" 
    FROM "sellout_storeproductsellout" "sales" 
    WHERE "sales"."product_id" IN (1,2) 
    AND "sales"."store_id" IN (3,4) 
    GROUP BY "sales"."product_id","sales"."sale_date"'
    """
    tester = QueryTester(query=query, expected_sql=expected_sql)
    tester.test()
