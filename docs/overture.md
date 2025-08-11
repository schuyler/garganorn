# Overture Maps Places Schema

This document describes the data model for Overture Maps Places, as of its writing.

## Overview

Overture Maps provides free and open geospatial map data normalized to a common schema. The places theme contains 64M+ point representations of real-world entities including businesses, schools, hospitals, landmarks, and points of interest. The data is sourced from Meta and Microsoft and is available under a CDLA Permissive 2.0 license.

## Places Dataset Schema

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
| `id` | String | ✓ | Unique identifier for the place following Overture's GERS (Global Entity Reference System) |
| `geometry` | Point | ✓ | Place's geometry which MUST be a Point as defined by GeoJSON schema |
| `names` | Object | | Names associated with the place including primary, common (multilingual), and rules-based variants |
| `names.primary` | String | | The primary name of the place |
| `names.common` | Object | | Common names in different languages using ISO 639-1 codes as keys (e.g., `{"es": "Spanish name"}`) |
| `names.rules` | Array[Object] | | Rule-based name variants with `variant` type and `value` (e.g., `[{"variant": "short", "value": "NYC"}]`) |
| `categories` | Object | ✓ | The categories of the place using Overture's hierarchical taxonomy |
| `categories.primary` | String | ✓ | The primary or main category using dot notation (e.g., `eat_and_drink.restaurant.italian_restaurant`) |
| `categories.alternate` | Array[String] | | Alternate categories when a place fits multiple categories (e.g., bookstore and coffee shop) |
| `confidence` | Number | | Confidence of the place's existence as a number between 0 and 1 (0 = doesn't exist, 1 = definitely exists) |
| `websites` | Array[String] | | Website URLs of the place (format: URI) |
| `socials` | Array[String] | | Social media URLs of the place (format: URI) |
| `emails` | Array[String] | | Email addresses of the place (format: email) |
| `phones` | Array[String] | | Phone numbers of the place |
| `brand` | Object | | Brand information for the place (for chain/franchise locations) |
| `brand.names` | Object | | Brand names with same structure as place names (primary, common, rules) |
| `brand.names.primary` | String | | Primary brand name |
| `brand.names.common` | Object | | Brand names in different languages using ISO 639-1 codes |
| `brand.names.rules` | Array[Object] | | Rule-based brand name variants |
| `brand.wikidata` | String | | Wikidata identifier for the brand (e.g., "Q38076" for McDonald's) |
| `addresses` | Array[Object] | | Array of physical addresses associated with the place |
| `addresses[].freeform` | String | | Free-form address string as it would appear on mail |
| `addresses[].locality` | String | | City, town, or locality name |
| `addresses[].region` | String | | State, province, or region with country prefix for disambiguation (e.g., "US-NY", "CA-ON") |
| `addresses[].country` | String | | ISO 3166-1 alpha-2 country code (e.g., "US", "CA", "GB") |
| `addresses[].postcode` | String | | Postal code, ZIP code, or equivalent local postal identifier |

## Standard Overture Properties

All Overture features, including Places, include these standard properties:

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
| `theme` | String | ✓ | Always "places" for place features |
| `type` | String | ✓ | Always "place" for place features |
| `version` | Integer | ✓ | Version number of the feature |
| `sources` | Array[Object] | | Information about data sources used for this feature with `property`, `dataset`, and `record_id` fields |
| `bbox` | Object | | Bounding box with xmin, ymin, xmax, ymax values |

## Categories

Overture Places includes 2000+ possible category values with hierarchical structure. Categories follow a dot-notation hierarchy (e.g., `eat_and_drink.restaurant.italian_restaurant`). The complete category list is available at the [Overture Categories CSV](https://github.com/OvertureMaps/schema/blob/main/docs/schema/concepts/by-theme/places/overture_categories.csv).

## License

Data is available under CDLA Permissive 2.0 license.

## References

- [Official Overture Maps Places Documentation](https://docs.overturemaps.org/schema/reference/places/place/)
- [Places Data Guide](https://docs.overturemaps.org/guides/places/)
- [Complete Category List](https://github.com/OvertureMaps/schema/blob/main/docs/schema/concepts/by-theme/places/overture_categories.csv)
- [Schema Concepts](https://docs.overturemaps.org/schema/concepts/by-theme/places/)
- [Overture Maps Foundation](https://overturemaps.org/)