import unittest
from unittest.mock import Mock, patch

import requests

from grocery_pipeline.assets.bronze.bronze_exploration_daily import (
    KrogerTokenExpired,
    KrogerTokenManager,
    _row_insert_id,
    search_products,
)


class KrogerTokenManagerTests(unittest.TestCase):
    def test_reuses_token_until_refresh(self):
        response = Mock()
        response.json.return_value = {"access_token": "token-1", "expires_in": 1800}
        response.raise_for_status.return_value = None
        session = Mock()
        session.post.return_value = response

        tokens = KrogerTokenManager(session)

        self.assertEqual(tokens.get(), "token-1")
        self.assertEqual(tokens.get(), "token-1")
        self.assertEqual(session.post.call_count, 1)

    def test_refresh_fetches_a_new_token(self):
        first = Mock()
        first.json.return_value = {"access_token": "token-1", "expires_in": 1800}
        first.raise_for_status.return_value = None
        second = Mock()
        second.json.return_value = {"access_token": "token-2", "expires_in": 1800}
        second.raise_for_status.return_value = None
        session = Mock()
        session.post.side_effect = [first, second]

        tokens = KrogerTokenManager(session)

        self.assertEqual(tokens.get(), "token-1")
        self.assertEqual(tokens.refresh(), "token-2")


class SearchProductsTests(unittest.TestCase):
    def test_unauthorized_response_requests_token_refresh(self):
        response = Mock(status_code=401)
        session = Mock()
        session.get.return_value = response

        with self.assertRaises(KrogerTokenExpired):
            search_products("expired", "bananas", "store", session=session)

    @patch("grocery_pipeline.assets.bronze.bronze_exploration_daily.time.sleep")
    def test_retries_transient_server_error(self, _sleep):
        unavailable = Mock(status_code=503, headers={})
        success = Mock(status_code=200, headers={})
        success.json.return_value = {"data": [{"productId": "1"}]}
        success.raise_for_status.return_value = None
        session = Mock()
        session.get.side_effect = [unavailable, success]

        products = search_products("token", "bananas", "store", session=session)

        self.assertEqual(products, [{"productId": "1"}])
        self.assertEqual(session.get.call_count, 2)


class InsertIdTests(unittest.TestCase):
    def test_insert_id_is_stable_for_a_daily_product_key(self):
        row = {
            "snapshot_date": "2026-11-11",
            "location_id": "70300022",
            "search_term": "bananas",
            "product_id": "123",
            "price": 1.99,
        }
        updated_price = {**row, "price": 2.09}

        self.assertEqual(_row_insert_id(row), _row_insert_id(updated_price))
        self.assertEqual(len(_row_insert_id(row)), 64)


if __name__ == "__main__":
    unittest.main()
