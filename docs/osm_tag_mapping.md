# OSM Tag Mapping Analysis for Garganorn

## How Categories Work in Garganorn Today

**FSQ**: Categories stored as `fsq_category_ids` (array of BSON IDs like `"4bf58dd8d48988d1c4941735"`) and `fsq_category_labels` (array of breadcrumb strings like `"Dining and Drinking > Restaurant > Italian Restaurant"`). The IDF computation unnests the `fsq_category_ids` array and computes `ln(total_places / places_with_category)`. The resulting IDF score contributes 40% of the importance score (density contributes 60%).

**Overture**: Categories stored as `categories.primary` (a dot-notation string like `eat_and_drink.restaurant.italian_restaurant`) and `categories.alternate` (array). IDF is computed on `categories.primary` only.

Both use the category as an opaque string for IDF. No cross-source normalization exists today.

---

## A. Coverage Comparison Table

| Place Type | FSQ Category (Level 1 > 2) | Overture Category | OSM Tag(s) |
|---|---|---|---|
| **Restaurant** | Dining and Drinking > Restaurant | eat_and_drink.restaurant | amenity=restaurant |
| **Fast Food** | Dining and Drinking > Fast Food | eat_and_drink.restaurant.fast_food_restaurant | amenity=fast_food |
| **Cafe/Coffee** | Dining and Drinking > Cafe | eat_and_drink.cafe | amenity=cafe |
| **Bar/Pub** | Dining and Drinking > Bar | eat_and_drink.bar | amenity=bar, amenity=pub |
| **Ice Cream** | Dining and Drinking > Ice Cream | eat_and_drink.restaurant.ice_cream_parlor | amenity=ice_cream |
| **Hotel** | Travel and Transportation > Hotel | accommodation.hotel | tourism=hotel |
| **Motel** | Travel and Transportation > Motel | accommodation.motel | tourism=motel |
| **Hostel** | Travel and Transportation > Hostel | accommodation.hostel | tourism=hostel |
| **Guest House/B&B** | Travel and Transportation > B&B | accommodation.bed_and_breakfast | tourism=guest_house |
| **Campground** | Outdoors > Campground | accommodation.campground | tourism=camp_site |
| **Supermarket** | Shop & Service > Grocery | retail.food.grocery_store_supermarket | shop=supermarket |
| **Convenience Store** | Shop & Service > Convenience Store | retail.food.convenience_store | shop=convenience |
| **Clothing Store** | Shop & Service > Clothing Store | retail.shopping.clothing_store | shop=clothes |
| **Bookstore** | Shop & Service > Bookstore | retail.shopping.bookstore | shop=books |
| **Electronics** | Shop & Service > Electronics | retail.shopping.electronics_store | shop=electronics |
| **Hardware/DIY** | Shop & Service > Hardware Store | retail.shopping.hardware_store | shop=hardware, shop=doityourself |
| **Bakery** | Shop & Service > Bakery | retail.food.bakery | shop=bakery |
| **Pharmacy** | Health > Pharmacy | retail.pharmacy | amenity=pharmacy, healthcare=pharmacy |
| **Bank** | Business > Bank | financial_service.bank_credit_union | amenity=bank |
| **ATM** | Business > ATM | financial_service.atms | amenity=atm |
| **Hospital** | Health > Hospital | health_and_medical.hospital | amenity=hospital, healthcare=hospital |
| **Doctor** | Health > Doctor | health_and_medical.doctor | amenity=doctors, healthcare=doctor |
| **Dentist** | Health > Dentist | health_and_medical.dentist | amenity=dentist, healthcare=dentist |
| **Veterinarian** | Health > Vet | pets.veterinarian | amenity=veterinary |
| **School** | Education > School | education.school | amenity=school |
| **University** | Education > University | education.college_university | amenity=university |
| **Library** | Arts & Entertainment > Library | education.school.library | amenity=library |
| **Museum** | Arts & Entertainment > Museum | attractions_and_activities.museum | tourism=museum |
| **Art Gallery** | Arts & Entertainment > Art Gallery | attractions_and_activities.art_gallery | tourism=gallery |
| **Theater** | Arts & Entertainment > Theater | arts_and_entertainment.theaters_and_performance_venues | amenity=theatre |
| **Cinema** | Arts & Entertainment > Movie Theater | arts_and_entertainment.cinema | amenity=cinema |
| **Park** | Outdoors > Park | attractions_and_activities.park | leisure=park |
| **Playground** | Outdoors > Playground | (none specific) | leisure=playground |
| **Sports Center** | Sports > Gym / Fitness | active_life.sports_and_recreation_venue | leisure=sports_centre, leisure=fitness_centre |
| **Swimming Pool** | Sports > Pool | active_life.sports_and_recreation_venue.swimming_pool | leisure=swimming_pool |
| **Golf Course** | Sports > Golf Course | active_life.sports_and_recreation_venue.golf_course | leisure=golf_course |
| **Zoo** | Arts & Entertainment > Zoo | attractions_and_activities.zoo | tourism=zoo |
| **Aquarium** | Arts & Entertainment > Aquarium | attractions_and_activities.aquarium | tourism=aquarium |
| **Theme Park** | Arts & Entertainment > Theme Park | attractions_and_activities.amusement_park | tourism=theme_park |
| **Place of Worship** | Spiritual > Church/Mosque/Temple | (none specific) | amenity=place_of_worship |
| **Post Office** | Shop & Service > Post Office | (none specific) | amenity=post_office |
| **Police Station** | Government > Police | (none specific) | amenity=police |
| **Fire Station** | Government > Fire Station | (none specific) | amenity=fire_station |
| **Gas/Fuel Station** | Travel > Gas Station | automotive.gas_station | amenity=fuel |
| **Car Wash** | Shop & Service > Car Wash | automotive.automotive_services_and_repair.car_wash | amenity=car_wash |
| **Car Repair** | Shop & Service > Auto Garage | automotive.automotive_services_and_repair | shop=car_repair |
| **Hairdresser/Salon** | Shop & Service > Salon | beauty_and_spa.hair_salon | shop=hairdresser |
| **Spa/Massage** | Shop & Service > Spa | beauty_and_spa.spas | shop=massage, leisure=sauna |
| **Laundry** | Shop & Service > Laundromat | (none specific) | shop=laundry |
| **Nightclub** | Nightlife > Night Club | arts_and_entertainment.dance_club | amenity=nightclub |
| **Casino** | Arts & Entertainment > Casino | arts_and_entertainment.casino | amenity=casino |
| **Marina** | Outdoors > Marina | attractions_and_activities.marina | leisure=marina |
| **Airport** | Travel > Airport | (none specific) | aeroway=aerodrome |
| **Train Station** | Travel > Train Station | (none specific) | railway=station |
| **Bus Station** | Travel > Bus Station | (none specific) | amenity=bus_station |
| **Castle** | (none specific) | attractions_and_activities.castle | historic=castle |
| **Monument** | (none specific) | attractions_and_activities.monument | historic=monument |
| **Lighthouse** | (none specific) | attractions_and_activities.lighthouse | man_made=lighthouse |
| **Beach** | Outdoors > Beach | attractions_and_activities.beach | natural=beach |
| **Mountain Peak** | Outdoors > Mountain | (none specific) | natural=peak |
| **City/Town/Village** | (none specific) | (none specific) | place=city/town/village/hamlet |

---

## B. OSM Tags to Include

### Tier 1: Core POI tags (direct FSQ/Overture equivalents)

| Tag Key | Values to Include | Global Count | Rationale |
|---|---|---|---|
| **amenity** | All _except_ infrastructure values (see exclusion list below) | ~33M total; ~15M after filtering | Core POI tag. Covers restaurants, schools, hospitals, banks, places of worship, fuel stations, etc. Best overlap with FSQ/Overture. |
| **shop** | All _except_ `yes`, `vacant` | ~6.9M | All shop types are searchable places. Covers retail, services, food shops. Strong FSQ/Overture overlap. |
| **tourism** | All | ~3.9M | Hotels, museums, attractions, camps, viewpoints. Nearly 1:1 with FSQ travel categories and Overture accommodation/attractions. |
| **leisure** | `park`, `sports_centre`, `fitness_centre`, `swimming_pool`, `golf_course`, `stadium`, `sports_hall`, `marina`, `nature_reserve`, `garden`, `playground`, `dog_park`, `ice_rink`, `water_park`, `miniature_golf`, `bowling_alley`, `beach_resort`, `resort`, `horse_riding`, `dance`, `sauna`, `amusement_arcade`, `adult_gaming_centre`, `trampoline_park`, `escape_game`, `hackerspace` | ~6M of 11.2M | Named leisure facilities are searchable places. Exclude `pitch` (2.7M unnamed sports fields), `swimming_pool` may need a `name=*` filter since many are private backyard pools. |
| **office** | All _except_ `yes` | ~1.3M | Government offices, law firms, insurance, embassies. Maps to FSQ "Business" and Overture "financial_service"/"private_establishments_and_corporates". |

### Tier 2: Important POI tags (partial FSQ/Overture overlap)

| Tag Key | Values to Include | Global Count | Rationale |
|---|---|---|---|
| **craft** | All _except_ `yes` | ~368K | Workshops, breweries, wineries, carpenters. Maps to some FSQ "Shop & Service" and Overture "business_to_business" categories. |
| **healthcare** | All _except_ `yes` | ~1M | Supplements `amenity=hospital/doctors/dentist`. The `healthcare` tag is the modern preferred tag. Many features have both. |
| **historic** | `castle`, `monument`, `memorial`, `archaeological_site`, `ruins`, `fort`, `manor`, `church`, `city_gate`, `building`, `mine`, `wreck` | ~1.2M of 2.2M | Named historic sites are gazetteer-worthy. Exclude `wayside_cross` (228K, mostly unnamed), `wayside_shrine` (161K, mostly unnamed), `boundary_stone` (94K), `charcoal_pile` (51K), `bomb_crater` (20K). |
| **natural** | `peak`, `beach`, `spring`, `bay`, `cave_entrance`, `volcano`, `glacier`, `hot_spring`, `cape`, `hill`, `valley`, `saddle`, `ridge`, `geyser`, `arch`, `gorge`, `rock` (when named) | ~2M of 90M | Named natural features are searchable. The vast majority of `natural` tags (tree=32M, water=22M, wood=12M) are mapping features, not named places. |
| **man_made** | `lighthouse`, `tower`, `pier`, `observatory`, `windmill`, `water_tower`, `works`, `chimney`, `obelisk`, `watermill`, `beacon` (when named) | ~1M of 9.5M | Named man-made landmarks. Exclude infrastructure items like `storage_tank` (826K), `manhole` (489K), `cutline` (608K), `surveillance` (428K), `utility_pole` (398K), `pipeline` (355K), `survey_point` (390K), `petroleum_well` (318K), `street_cabinet` (297K). |
| **aeroway** | `aerodrome`, `terminal`, `heliport` | ~57K of 1.1M | Airports and terminals are key gazetteer entries. Exclude airfield infrastructure (taxiway=288K, navigationaid=236K, parking_position=125K, runway=66K, etc.). |
| **railway** | `station`, `halt`, `tram_stop`, `subway_entrance` | ~210K of 8.2M | Transit stations are key search targets. Exclude track geometry (rail=2.8M), signals (476K), switches (1.2M), crossings (996K), etc. |
| **public_transport** | `station`, `stop_position`, `platform` | Selected from 6.3M | Transit stops. Consider `station` only, since `stop_position` and `platform` often duplicate railway/amenity data. |

### Tier 3: Supplementary tags (limited FSQ/Overture overlap)

| Tag Key | Values to Include | Global Count | Rationale |
|---|---|---|---|
| **place** | `city`, `town`, `village`, `hamlet`, `suburb`, `neighbourhood`, `quarter`, `island`, `islet`, `locality`, `isolated_dwelling`, `farm`, `square` | ~9.3M | Populated places. Not POIs, but core gazetteer content. See special considerations below. |
| **sport** | Use as _secondary tag_ only, not as primary selector | ~3.1M | Always paired with `leisure=*`. Useful for subcategorizing (e.g., `leisure=sports_centre` + `sport=tennis`). |
| **emergency** | `ambulance_station` only | ~16K of 3.2M | Fire hydrants (2.4M) and defibrillators (155K) are infrastructure, not places. `ambulance_station` is the only place-type. But `amenity=fire_station` and `amenity=police` already cover emergency services. |

---

## C. OSM Tags to Exclude

| Tag Key | Global Count | Rationale |
|---|---|---|
| **highway** | 292M | Road network geometry. Not places. |
| **building** | 681M | Structural outlines. The vast majority are unnamed residential buildings. Named buildings should be captured through their functional tags (amenity, shop, tourism, etc.). |
| **barrier** | 32M | Fences, walls, gates. Physical infrastructure. |
| **boundary** | (via relation) | Administrative boundaries. Populated places are covered by `place=*` nodes. |
| **waterway** | 39M | Rivers, streams, canals. Linear water features, not named places (though some named rivers could come from `natural=water` + `name=*`). |
| **power** | 49M | Power lines, substations, generators. Infrastructure. |
| **landuse** | 50M | Land use polygons (residential, commercial, forest). Zoning, not named places. |
| **natural** (most values) | ~88M of 90M | `tree` (32M), `water` (22M), `wood` (12M), `scrub` (5.4M), `wetland` (4.5M), `grassland` (2.3M), `coastline` (1.3M), `bare_rock` (1.3M). These are land cover, not named places. |
| **amenity** (infrastructure values) | ~18M of 33M | `parking` (6.5M), `parking_space` (4.6M), `bench` (3.2M), `waste_basket` (1.1M), `bicycle_parking` (875K), `shelter` (642K), `recycling` (550K), `toilets` (500K), `post_box` (404K), `drinking_water` (351K), `vending_machine` (341K), `waste_disposal` (298K), `hunting_stand` (297K), `parking_entrance` (263K), `grit_bin`, `give_box`, `bbq`. These are street furniture/infrastructure. |
| **man_made** (infrastructure values) | ~8M of 9.5M | `storage_tank`, `cutline`, `manhole`, `mast`, `surveillance`, `utility_pole`, `survey_point`, `pipeline`, `petroleum_well`, `street_cabinet`, `embankment`, `flagpole`, `bunker_silo`, etc. |
| **emergency** (most values) | ~3.2M of 3.2M | `fire_hydrant` (2.4M), `defibrillator` (155K), `phone` (72K), `siren` (24K), etc. Infrastructure, not places. |
| **military** | ~170K | Military installations. Sensitive and often access-restricted. |
| **telecom** | ~235K | Telecom infrastructure. |
| **route** | (relations) | Route relations, not places. |

---

## D. Special Considerations

### D1. The `place=*` tag

**Recommendation: Include selectively.**

`place=*` nodes represent populated places: cities (15K), towns (117K), villages (1.7M), hamlets (2.1M), suburbs (160K), neighbourhoods (531K), islands (100K), etc. Total: ~9.3M features.

These are not POIs in the Foursquare/Overture sense -- they're settlement names. However, they are absolutely "places" in the gazetteer sense. A user searching for "Springfield" or "Montmartre" expects to find these.

Neither FSQ nor Overture systematically covers populated place nodes. This is something uniquely valuable that OSM brings to the table.

For the import, consider:
- **Include**: `city`, `town`, `village`, `hamlet`, `suburb`, `neighbourhood`, `quarter`, `island`, `square`
- **Exclude**: `locality` (1.9M -- often unnamed/informal), `isolated_dwelling` (850K -- individual houses), `farm` (280K -- may overlap with other tags), `plot` (308K), `city_block` (102K), `islet` (700K -- mostly very small uninhabited features), administrative divisions (`municipality`, `county`, `district`, `region`, `state`, `province`, `borough`)
- Require `name=*` on all `place=*` features

### D2. Named `building=*` features

**Recommendation: Do not use `building=*` as a primary selector.**

There are 681M building features. Even filtering to named buildings would yield tens of millions of residential buildings with house numbers but no meaningful names. Buildings that are gazetteer-worthy (churches, museums, stadiums) will be captured through their functional tags (`amenity=*`, `tourism=*`, `leisure=*`, etc.).

### D3. `natural=*` features

**Recommendation: Include a curated subset, require `name=*`.**

Named natural features (peaks, beaches, bays, volcanoes, hot springs) are high-value gazetteer entries that neither FSQ nor Overture covers well. The key filter is `name=*`:
- `natural=peak` + `name=*`: Most peaks are named. 1.08M features globally.
- `natural=beach` + `name=*`: ~242K features.
- `natural=bay`: ~89K, most named.
- `natural=volcano`, `natural=hot_spring`, `natural=geyser`, `natural=cave_entrance`: Low counts, high value.
- `natural=spring`: ~276K. Named springs can be significant landmarks.

Exclude land cover types (tree, wood, water, scrub, wetland, grassland, etc.) even when named.

### D4. Features as ways/relations (polygons)

Many features exist only as closed ways or relations (e.g., parks, nature reserves, large buildings used as amenities). For these, the import should compute a centroid using `ST_Centroid(geometry)` to produce a point location. This is exactly what Garganorn already does for Overture data:
```sql
st_y(st_centroid(geometry))::decimal(10,6)::varchar as latitude
st_x(st_centroid(geometry))::decimal(10,6)::varchar as longitude
```

DuckDB spatial can handle this natively. The OSM extract tool (e.g., `osm2pgsql`, `ogr2ogr`, or a Parquet-based pipeline) should preserve geometry type so centroids can be computed at import time.

### D5. Subcategorization tags

Several OSM tags provide subcategorization similar to FSQ's deep hierarchy:
- `cuisine=*` on `amenity=restaurant/fast_food/cafe` (e.g., `cuisine=italian`, `cuisine=chinese`)
- `sport=*` on `leisure=sports_centre/pitch/stadium` (e.g., `sport=soccer`, `sport=tennis`)
- `religion=*` and `denomination=*` on `amenity=place_of_worship`
- `healthcare:speciality=*` on `healthcare=doctor`
- `shop` already provides fine-grained subtypes

These should be extracted as supplementary attributes, not as primary category selectors. They can enrich the `attributes` blob in the output but should not be used for IDF computation.

---

## E. Recommended Category Normalization Approach

### Recommendation: Use `{key}={value}` as the category string

**For IDF computation**, use the concatenated form `amenity=restaurant`, `shop=supermarket`, `tourism=hotel`, etc. This approach:

1. **Requires no external mapping.** The category is derived directly from the OSM data.
2. **Works with the existing IDF pipeline.** The `build-idf.sh` script just needs a single string per feature -- currently `fsq_category_ids` elements or `categories.primary`. Using `amenity=restaurant` is structurally identical.
3. **Provides meaningful IDF scores.** Rare categories like `amenity=planetarium` will get high IDF; common ones like `amenity=parking` (if included) will get low IDF. This is exactly what you want for importance scoring.
4. **Preserves OSM's native granularity.** OSM's tag system is already at a good level of specificity for IDF. `shop=bakery` vs `shop=supermarket` have different frequencies and should have different IDF scores.

**For multi-tagged features**, choose the primary category using tag priority:
1. `amenity` > `shop` > `tourism` > `leisure` > `office` > `craft` > `healthcare` > `historic` > `natural` > `man_made` > `aeroway` > `railway` > `place`
2. If a feature has both `amenity=restaurant` and `tourism=hotel`, use `amenity=restaurant` as primary.
3. Store the alternate categories as an array (similar to Overture's `categories.alternate`).

**Do not attempt to map to FSQ or Overture taxonomies.** The mapping would be lossy, subjective, and maintenance-intensive. Each data source should use its own native category system for IDF. Cross-source category normalization, if needed later, belongs in a separate mapping table -- not baked into the import.

### OSM-specific schema additions

For the OSM Database class, the `categories` attribute should look like:
- `osm_category`: Primary category string (e.g., `"amenity=restaurant"`)
- `osm_category_alt`: Array of alternate category strings (e.g., `["cuisine=italian"]`)
- `osm_id`: The OSM feature ID (node/way/relation + numeric ID)
- `osm_type`: `"node"`, `"way"`, or `"relation"`

---

## Summary Statistics

Estimated global feature counts for recommended OSM import (before regional bbox filter):

| Tag Group | Estimated Features (after filtering) |
|---|---|
| amenity (POI values only) | ~15M |
| shop | ~6.9M |
| tourism | ~3.9M |
| leisure (place values only) | ~6M |
| place (settlements only) | ~5M |
| historic (curated) | ~1.2M |
| office | ~1.3M |
| healthcare | ~1M |
| craft | ~368K |
| natural (named, curated) | ~2M |
| man_made (curated, named) | ~1M |
| railway (stations only) | ~210K |
| aeroway (airports/terminals) | ~57K |
| public_transport (stations) | ~100K |
| **Rough total (with overlap)** | **~35-40M** |

After deduplication (many features carry multiple tags -- a hospital might have `amenity=hospital` + `healthcare=hospital`), and after requiring `name=*` on applicable features, the effective count would be **~20-25M globally**, which is comparable in magnitude to FSQ's 100M (FSQ includes many chain locations and temporary venues that OSM doesn't map) and Overture's 64M.

For a regional extract (e.g., a single country or US state), these numbers scale proportionally. A typical US state might yield 50K-500K OSM places depending on size and mapping density.
