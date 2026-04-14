import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import crm_mailer
from campaign_service import build_campaign_asset_path


class CampaignAssetTests(unittest.TestCase):
    def test_build_campaign_asset_path_uses_campaign_folder(self) -> None:
        campaign = {"id": "installateur_wien"}
        self.assertEqual(
            build_campaign_asset_path(campaign, "assets", "megaphonia-web-flyer.png"),
            "campaigns/installateur_wien/assets/megaphonia-web-flyer.png",
        )

    def test_extract_inline_images_uses_postgres_asset_fallback(self) -> None:
        html = "Hi {{img:campaigns/installateur_wien/assets/megaphonia-web-flyer.png|Flyer}}"
        campaign = {"id": "installateur_wien"}
        asset = {
            "data_bytes": b"png-bytes",
            "content_type": "image/png",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(crm_mailer.backend, "is_postgres_backend", return_value=True), patch.object(
                crm_mailer.backend,
                "postgres_get_campaign_asset",
                return_value=asset,
            ) as get_asset:
                rendered, images = crm_mailer._extract_inline_images(html, tmpdir, campaign=campaign)

        self.assertIn("cid:", rendered)
        self.assertEqual(len(images), 1)
        _, path, data, content_type = images[0]
        self.assertEqual(path, "campaigns/installateur_wien/assets/megaphonia-web-flyer.png")
        self.assertEqual(data, b"png-bytes")
        self.assertEqual(content_type, "image/png")
        get_asset.assert_called_once_with(
            "installateur_wien",
            "campaigns/installateur_wien/assets/megaphonia-web-flyer.png",
        )

    def test_extract_inline_images_prefers_local_file_when_available(self) -> None:
        html = "{{img:campaigns/installateur_wien/assets/local-flyer.png|Flyer}}"
        relative_path = Path("campaigns/installateur_wien/assets/local-flyer.png")

        with tempfile.TemporaryDirectory() as tmpdir:
            absolute_path = Path(tmpdir) / relative_path
            absolute_path.parent.mkdir(parents=True, exist_ok=True)
            absolute_path.write_bytes(b"local-png")

            rendered, images = crm_mailer._extract_inline_images(html, tmpdir, campaign={"id": "installateur_wien"})

        self.assertIn("cid:", rendered)
        self.assertEqual(len(images), 1)
        _, _, data, _ = images[0]
        self.assertEqual(data, b"local-png")


if __name__ == "__main__":
    unittest.main()
