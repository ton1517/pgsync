"""Tests for `pgsync` package."""

import mock
import psycopg2
import pytest
import sqlalchemy as sa

from pgsync.base import subtransactions
from pgsync.settings import NTHREADS_POLLDB
from pgsync.singleton import Singleton
from pgsync.sync import Sync

from .testing_utils import assert_resync_empty, noop, search, sort_list


@pytest.mark.usefixtures("table_creator")
class TestThroughTableWithoutPK(object):
    """Root and nested childred node tests."""

    @pytest.fixture(scope="function")
    def data(
        self,
        sync,
        shop_cls,
        product_cls,
        shop_product_cls,
    ):
        session = sync.session
        session.execute(
            sa.DDL(
                "ALTER TABLE shop_product REPLICA IDENTITY "
                "USING INDEX unique_idx_shop_product"
            )
        )

        shops = [
            shop_cls(id=1, name="Shop_1"),
            shop_cls(id=2, name="Shop_2"),
        ]

        products = [
            product_cls(id=1, name="Product_1"),
            product_cls(id=2, name="Product_2"),
            product_cls(id=3, name="Product_3"),
            product_cls(id=4, name="Product_4"),
            product_cls(id=5, name="Product_5"),
        ]

        shop_products = [
            shop_product_cls(shop=shops[0], product=products[0]),
            shop_product_cls(shop=shops[1], product=products[0]),
            shop_product_cls(shop=shops[1], product=products[1]),
            shop_product_cls(shop=shops[1], product=products[2]),
        ]

        with subtransactions(session):
            conn = session.connection().engine.connect().connection
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
            cursor = conn.cursor()
            channel = sync.database
            cursor.execute(f"UNLISTEN {channel}")

        with subtransactions(session):
            session.add_all(shops)
            session.add_all(products)
            session.add_all(shop_products)

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )

        Singleton._instances = {}

        yield (
            shops,
            products,
            shop_products,
        )

        with subtransactions(session):
            conn = session.connection().engine.connect().connection
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
            cursor = conn.cursor()
            channel = session.connection().engine.url.database
            cursor.execute(f"UNLISTEN {channel}")

        with subtransactions(session):
            sync.truncate_tables(
                [
                    shop_cls.__table__.name,
                    product_cls.__table__.name,
                    shop_product_cls.__table__.name,
                ]
            )

        sync.logical_slot_get_changes(
            f"{sync.database}_testdb",
            upto_nchanges=None,
        )

        try:
            sync.search_client.teardown(index="testdb")
            sync.search_client.close()
        except Exception:
            raise

        sync.redis.delete()
        session.connection().engine.connect().close()
        session.connection().engine.dispose()
        sync.search_client.close()

    @pytest.fixture(scope="function")
    def nodes(self):
        return {
            "table": "shop",
            "columns": ["id", "name"],
            "children": [
                {
                    "table": "product",
                    "columns": ["id", "name"],
                    "label": "products",
                    "relationship": {
                        "type": "one_to_many",
                        "variant": "object",
                        "through_tables": ["shop_product"],
                        "primary_key": ["shop_id", "product_id"],
                    },
                },
            ],
        }

    def assert_docs(self, docs, expected):
        docs = sorted(docs, key=lambda k: k["id"])
        for i, doc in enumerate(docs):
            for key in [
                "_meta",
                "id",
                "name",
                "products",
            ]:
                if key == "products":
                    assert sorted(doc[key], key=lambda k: k["id"]) == sorted(
                        expected[i][key], key=lambda k: k["id"]
                    )
                else:
                    assert doc[key] == expected[i][key]

    def test_sync(self, sync, nodes, data):
        """Test regular sync produces the correct result."""
        sync.tree.__post_init__()
        sync.nodes = nodes
        sync.root = sync.tree.build(nodes)
        sync.search_client.bulk(
            sync.index, [sort_list(doc) for doc in sync.sync()]
        )
        sync.search_client.refresh("testdb")
        docs = search(sync.search_client, "testdb")
        expected = [
            {
                "_meta": {
                    "product": {"id": [1]},
                    "shop_product": {"shop_id": [1], "product_id": [1]},
                },
                "id": 1,
                "name": "Shop_1",
                "products": [{"id": 1, "name": "Product_1"}],
            },
            {
                "_meta": {
                    "product": {"id": [1, 2, 3]},
                    "shop_product": {
                        "shop_id": [2],
                        "product_id": [1, 2, 3],
                    },
                },
                "id": 2,
                "name": "Shop_2",
                "products": [
                    {"id": 1, "name": "Product_1"},
                    {"id": 2, "name": "Product_2"},
                    {"id": 3, "name": "Product_3"},
                ],
            },
        ]

        self.assert_docs(docs, expected)

        assert_resync_empty(sync, nodes)

    def test_insert_through_child_op(
        self, nodes, data, shop_cls, product_cls, shop_product_cls
    ):
        # insert a new through child with op
        document = {
            "index": "testdb",
            "database": "testdb",
            "nodes": nodes,
        }

        sync = Sync(document)
        sync.tree.build(nodes)
        session = sync.session

        with subtransactions(session):
            session.execute(
                product_cls.__table__.insert().values(
                    id=111, name="ProductAAA"
                )
            )

        docs = [sort_list(doc) for doc in sync.sync()]
        assert len(docs) == 2
        assert len(docs[0]["_source"]["products"]) == 1
        assert docs[0]["_source"]["products"][0]["id"] == 1
        sync.checkpoint = sync.txid_current

        def pull():
            txmin = sync.checkpoint
            txmax = sync.txid_current
            sync.logical_slot_changes(txmin=txmin, txmax=txmax)

        def poll_redis():
            return []

        def poll_db():
            with subtransactions(session):
                session.execute(
                    shop_product_cls.__table__.insert().values(
                        shop_id=1, product_id=111
                    )
                )

        with mock.patch("pgsync.sync.Sync.poll_redis", side_effect=poll_redis):
            with mock.patch("pgsync.sync.Sync.poll_db", side_effect=poll_db):
                with mock.patch("pgsync.sync.Sync.pull", side_effect=pull):
                    with mock.patch(
                        "pgsync.sync.Sync.truncate_slots",
                        side_effect=noop,
                    ):
                        with mock.patch(
                            "pgsync.sync.Sync.status",
                            side_effect=noop,
                        ):
                            sync.receive(NTHREADS_POLLDB)
                            sync.search_client.refresh("testdb")

        sync.search_client.bulk(
            sync.index, [sort_list(doc) for doc in sync.sync()]
        )
        sync.search_client.refresh("testdb")
        docs = search(sync.search_client, "testdb")

        self.assert_docs(
            docs,
            expected=[
                {
                    "_meta": {
                        "product": {"id": [1, 111]},
                        "shop_product": {
                            "shop_id": [1],
                            "product_id": [1, 111],
                        },
                    },
                    "id": 1,
                    "name": "Shop_1",
                    "products": [
                        {"id": 1, "name": "Product_1"},
                        {"id": 111, "name": "ProductAAA"},
                    ],
                },
                {
                    "_meta": {
                        "product": {"id": [1, 2, 3]},
                        "shop_product": {
                            "shop_id": [2],
                            "product_id": [1, 2, 3],
                        },
                    },
                    "id": 2,
                    "name": "Shop_2",
                    "products": [
                        {"id": 1, "name": "Product_1"},
                        {"id": 2, "name": "Product_2"},
                        {"id": 3, "name": "Product_3"},
                    ],
                },
            ],
        )
        assert_resync_empty(sync, nodes)

        sync.search_client.close()

    def test_update_through_child_op(
        self,
        sync,
        nodes,
        data,
        shop_product_cls,
    ):
        # update a new through child with op
        document = {
            "index": "testdb",
            "database": "testdb",
            "nodes": nodes,
        }

        # 1. sync first to add the initial document
        sync = Sync(document)
        sync.search_client.bulk(
            sync.index, [sort_list(doc) for doc in sync.sync()]
        )

        session = sync.session
        with subtransactions(session):
            session.execute(
                shop_product_cls.__table__.update()
                .where(
                    sa.and_(
                        shop_product_cls.__table__.c.shop_id == 1,
                        shop_product_cls.__table__.c.product_id == 1,
                    )
                )
                .values(product_id=5)
            )

        sync.search_client.bulk(
            sync.index, [sort_list(doc) for doc in sync.sync()]
        )
        sync.search_client.refresh("testdb")

        docs = search(sync.search_client, "testdb")
        self.assert_docs(
            docs,
            expected=[
                {
                    "_meta": {
                        "product": {"id": [5]},
                        "shop_product": {"shop_id": [1], "product_id": [5]},
                    },
                    "id": 1,
                    "name": "Shop_1",
                    "products": [
                        {"id": 5, "name": "Product_5"},
                    ],
                },
                {
                    "_meta": {
                        "product": {"id": [1, 2, 3]},
                        "shop_product": {
                            "shop_id": [2],
                            "product_id": [1, 2, 3],
                        },
                    },
                    "id": 2,
                    "name": "Shop_2",
                    "products": [
                        {"id": 1, "name": "Product_1"},
                        {"id": 2, "name": "Product_2"},
                        {"id": 3, "name": "Product_3"},
                    ],
                },
            ],
        )

        assert_resync_empty(sync, nodes)
        sync.search_client.close()

    def test_delete_through_child_op(
        self, sync, nodes, data, shop_product_cls
    ):
        # delete a new through child with op
        document = {
            "index": "testdb",
            "database": "testdb",
            "nodes": nodes,
        }

        # 1. sync first to add the initial document
        sync = Sync(document)
        sync.search_client.bulk(
            sync.index, [sort_list(doc) for doc in sync.sync()]
        )

        session = sync.session

        with subtransactions(session):
            session.execute(
                shop_product_cls.__table__.delete().where(
                    sa.and_(
                        shop_product_cls.__table__.c.shop_id == 2,
                        shop_product_cls.__table__.c.product_id == 2,
                    )
                )
            )

        sync.search_client.bulk(
            sync.index, [sort_list(doc) for doc in sync.sync()]
        )
        sync.search_client.refresh("testdb")
        docs = search(sync.search_client, "testdb")

        self.assert_docs(
            docs,
            expected=[
                {
                    "_meta": {
                        "product": {"id": [1]},
                        "shop_product": {"shop_id": [1], "product_id": [1]},
                    },
                    "id": 1,
                    "name": "Shop_1",
                    "products": [
                        {"id": 1, "name": "Product_1"},
                    ],
                },
                {
                    "_meta": {
                        "product": {"id": [1, 3]},
                        "shop_product": {"shop_id": [2], "product_id": [1, 3]},
                    },
                    "id": 2,
                    "name": "Shop_2",
                    "products": [
                        {"id": 1, "name": "Product_1"},
                        {"id": 3, "name": "Product_3"},
                    ],
                },
            ],
        )

        assert_resync_empty(sync, nodes)
        sync.search_client.close()

    def test_delete_through_child_op2(self, nodes, data, shop_product_cls):
        # insert a new through child with op
        document = {
            "index": "testdb",
            "database": "testdb",
            "nodes": nodes,
        }

        sync = Sync(document)
        sync.search_client.bulk(
            sync.index, [sort_list(doc) for doc in sync.sync()]
        )
        session = sync.session
        sync.checkpoint = sync.txid_current

        def pull():
            txmin = sync.checkpoint
            txmax = sync.txid_current
            sync.logical_slot_changes(txmin=txmin, txmax=txmax)

        def poll_redis():
            return []

        def poll_db():
            with subtransactions(session):
                session.execute(
                    shop_product_cls.__table__.delete().where(
                        sa.and_(
                            shop_product_cls.__table__.c.shop_id == 2,
                            shop_product_cls.__table__.c.product_id == 2,
                        )
                    )
                )

        with mock.patch("pgsync.sync.Sync.poll_redis", side_effect=poll_redis):
            with mock.patch("pgsync.sync.Sync.poll_db", side_effect=poll_db):
                with mock.patch("pgsync.sync.Sync.pull", side_effect=pull):
                    with mock.patch(
                        "pgsync.sync.Sync.truncate_slots",
                        side_effect=noop,
                    ):
                        with mock.patch(
                            "pgsync.sync.Sync.status",
                            side_effect=noop,
                        ):
                            sync.receive(NTHREADS_POLLDB)
                            sync.search_client.refresh("testdb")

        sync.search_client.bulk(
            sync.index, [sort_list(doc) for doc in sync.sync()]
        )
        sync.search_client.refresh("testdb")
        docs = search(sync.search_client, "testdb")

        self.assert_docs(
            docs,
            expected=[
                {
                    "_meta": {
                        "product": {"id": [1]},
                        "shop_product": {"shop_id": [1], "product_id": [1]},
                    },
                    "id": 1,
                    "name": "Shop_1",
                    "products": [
                        {"id": 1, "name": "Product_1"},
                    ],
                },
                {
                    "_meta": {
                        "product": {"id": [1, 3]},
                        "shop_product": {"shop_id": [2], "product_id": [1, 3]},
                    },
                    "id": 2,
                    "name": "Shop_2",
                    "products": [
                        {"id": 1, "name": "Product_1"},
                        {"id": 3, "name": "Product_3"},
                    ],
                },
            ],
        )

        assert_resync_empty(sync, nodes)
        sync.search_client.close()
