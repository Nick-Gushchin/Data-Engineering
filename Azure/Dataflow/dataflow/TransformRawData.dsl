source(output(
		id as string,
		name as string,
		host_id as string,
		host_name as string,
		neighbourhood_group as string,
		neighbourhood as string,
		latitude as string,
		longitude as string,
		room_type as string,
		price as integer,
		minimum_nights as string,
		number_of_reviews as string,
		last_review as string,
		reviews_per_month as integer,
		calculated_host_listings_count as string,
		availability_365 as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false) ~> source1
source1 filter(price > 0) ~> filter1
filter1 derive(last_review = toDate(last_review, 'yyyy-MM-dd'),
		reviews_per_month = iif(isNull(reviews_per_month), 0, reviews_per_month)) ~> derivedColumn1
derivedColumn1 filter(!isNull(latitude) && !isNull(longitude)) ~> filter2
filter2 sink(allowSchemaDrift: true,
	validateSchema: false,
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> sink1