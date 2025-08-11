# Foursquare Open Source Places Schema

This document describes the data model for Foursquare Open Source Places, as of its writing.

## Overview

Foursquare's Open Source Places provides free data to accelerate geospatial innovation and insights. The dataset contains 22 core attributes across 100M+ global POIs with 1000+ place categories in 200+ countries and territories.

## Places Dataset Schema

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
| `fsq_place_id` | String | ✓ | The unique identifier of a Foursquare POI (formerly known as venueid or fsq_id). Use this ID to view a venue at foursquare.com by visiting: http://www.foursquare.com/v/{fsq_place_id} |
| `name` | String | ✓ | Business name of a POI |
| `latitude` | Decimal | ✓ | Foursquare latitudes delivered as decimal places (WGS84 datum), where the value does not exceed 6 decimal places |
| `longitude` | Decimal | ✓ | Foursquare longitudes delivered as decimal places (WGS84 datum), where the value does not exceed 6 decimal places |
| `address` | String | | User-entered street address of the venue |
| `locality` | String | | City, town or equivalent the POI is located in |
| `region` | String | | State, province, territory or equivalent. Abbreviations are used in the following countries (US, CA, AU, and BR). Remaining countries use full names |
| `postcode` | String | | Postal code of the POI, or equivalent (zip code in the US). Format will be localized based on country |
| `admin_region` | String | | Additional sub-division. Usually, but not always, a country sub-division (e.g., Scotland) |
| `post_town` | String | | Town/place employed in postal addressing. May not reflect the formal geographic location of a place |
| `po_box` | String | | Post Office Box |
| `country` | String | | 2 Letter ISO Country Code |
| `date_created` | Date | | The date the POI entered our database. This does not necessarily mean the POI actually opened on this date |
| `date_refreshed` | Date | | The date the POI last had any single reference refreshed from crawl, users or human validation |
| `date_closed` | Date | | The date the POI was marked as closed in our database. This does not necessarily mean the POI actually closed on this date |
| `tel` | String | | Telephone number of a POI with local formatting |
| `website` | String | | URL to the POI's (or the chain's) publicly available website |
| `email` | String | | Primary contact email address of organization, if available |
| `facebook_id` | String | | This POI's Facebook ID, if available |
| `instagram` | String | | This POI's Instagram handle, if available |
| `twitter` | String | | This POI's Twitter handle, if available |
| `fsq_category_ids` | Array[String] | | Array of BSON identifiers for the most granular category (or categories) available for this POI |
| `placemaker_url` | String | | URL for the Placemaker tool for this place |
| `placemaker_url` | String | | URL for the Placemaker tool for this place |
| `bbox` | Struct | | Bounding box containing xmin, ymin, xmax, ymax as double values |

## Categories Dataset Schema

The dataset includes Foursquare's proprietary taxonomy of 1000+ categories with hierarchical structure up to 6 levels deep. For the complete category listing, see the [official Foursquare Categories documentation](https://docs.foursquare.com/data-products/docs/categories).

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
| `category_id` | String | ✓ | The unique identifier of the Foursquare category; represented as a BSON ObjectId (16-character alphanumeric string) |
| `category_level` | Integer | ✓ | The number of levels within the category's hierarchy; accepted values 1-6 |
| `category_name` | String | ✓ | The name of the most granular category in the category hierarchy |
| `category_label` | String | ✓ | The exploded category hierarchy using > to indicate category breadcrumb |
| `level1_category_id` | String | | The unique identifier for the first level category in the hierarchy |
| `level1_category_name` | String | | The name for the first level category in the hierarchy |
| `level2_category_id` | String | | The unique identifier for the second level category in the hierarchy |
| `level2_category_name` | String | | The name for the second level category in the hierarchy |
| `level3_category_id` | String | | The unique identifier for the third level category in the hierarchy |
| `level3_category_name` | String | | The name for the third level category in the hierarchy |
| `level4_category_id` | String | | The unique identifier for the fourth level category in the hierarchy |
| `level4_category_name` | String | | The name for the fourth level category in the hierarchy |
| `level5_category_id` | String | | The unique identifier for the fifth level category in the hierarchy |
| `level5_category_name` | String | | The name for the fifth level category in the hierarchy |
| `level6_category_id` | String | | The unique identifier for the sixth level category in the hierarchy |
| `level6_category_name` | String | | The name for the sixth level category in the hierarchy |

## Location Data

Default geocode type is front door or rooftop, where available. These are derived by a combination of: Direct input from third party sources and Direct input of precise latitude/longitude (a pin drop) from initial user creation and correction.

## Categories

The dataset includes category hierarchy using > to indicate category breadcrumb with up to 6 levels of categorization. Categories are represented as BSON identifiers with hierarchical naming.

## Data Quality

Records are filtered to include only venues that:
- Have been refreshed after March 15, 2020 (`date_refreshed > '2020-03-15'`)
- Are not marked as closed (`date_closed is null`)

## License

Copyright 2024 Foursquare Labs, Inc. All rights reserved. Licensed under the Apache License, Version 2.0

## References

- [Official Foursquare OS Places Documentation](https://docs.foursquare.com/data-products/docs/places-os-data-schema)
- [Foursquare Open Source Portal](https://opensource.foursquare.com/os-places/)
- [Data Access Documentation](https://docs.foursquare.com/data-products/docs/access-fsq-os-places)