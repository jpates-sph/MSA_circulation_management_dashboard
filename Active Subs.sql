--- FROM subscription base SQL ---

-- package details for package type field
WITH latest_package_load AS (
	SELECT
		main_package,
		MAX(load_tmestp) AS max_load_tmestp
	FROM
		cir_ss_package_details
	GROUP BY main_package
),
latest_package_details AS (
	SELECT DISTINCT
		t1.main_package,
		t2.package_type
	FROM
		latest_package_load AS t1
	JOIN
		cir_ss_package_details AS t2
	ON t1.main_package = t2.main_package
		AND t1.max_load_tmestp = t2.load_tmestp
),

-- The full list of subscription records
subs_base_raw AS (
	SELECT DISTINCT
		-- Product Code Groupings (not available in Redshift) Hack
		t2.product_code_group_1,
		t1.account_no,
		t1.subscription_name,
		t1.subscription_creation_date,
		t1.subscription_sttdte,
		t1.subscription_enddte,
		t1.subscription_cttenddte,
		t1.main_package,
		t3.package_type,
		t1.product_code,
		t1.isVendorConv,
		t1.status_sfdc,
		t1.sub_group,
		t1.copies

	FROM
		cir_ss_subscription AS t1

	LEFT JOIN
		cir_ss_product_group AS t2
	ON
		t1.product_code = t2.product_code

	LEFT JOIN
    	latest_package_details AS t3
   ON
		t1.main_package = t3.main_package

	WHERE
		-- Only latest record updates
		t1.drv_update_no = 0
		-- removes erroneous deleted records
		AND (t1.subscription_creation_date < t1.subscription_enddte OR t1.subscription_enddte IS NULL)
		-- there are items without product codes. They won't work with the queries anyway.
		AND t1.product_code IS NOT NULL
		-- removes no revenue items
		AND t1.isfreelist = FALSE
		AND COALESCE(t1.other_payment_method,'') != 'Freelist'
		AND t1.status_sfdc NOT IN ('Deleted','Future Active')
),

-- fields used for CMG filter that will remove double counting of certain CMG packages
cmg_filters AS (
	SELECT
		main_package, product_code_1, product_code_2, product_code_3
	FROM
		cir_ss_publication_filters
),

-- CMG Filtered subscription records
subs_base AS (
	SELECT t1.*
	FROM
		subs_base_raw AS t1
	LEFT JOIN
		cmg_filters AS t2
	ON
		t1.main_package = t2.main_package
	WHERE
		t2.main_package IS NULL
		OR (t2.main_package IS NOT NULL
			AND t1.product_code IN (t2.product_code_1, t2.product_code_2, t2.product_code_3)
			)
),

-- various ordering of subscription records for later queries
subs_base_ordered_raw AS (
	SELECT
		ROW_NUMBER() OVER (
			PARTITION BY product_code_group_1, account_no
			ORDER BY subscription_creation_date, subscription_sttdte, subscription_enddte
		) AS sub_order,

		DENSE_RANK() OVER (
      	PARTITION BY product_code_group_1, account_no
         ORDER BY subscription_creation_date
      ) AS sub_dense_order_creation,

      DENSE_RANK() OVER (
         PARTITION BY product_code_group_1, account_no
         ORDER BY subscription_creation_date, subscription_sttdte
      ) AS sub_dense_order_creation_start,

		product_code_group_1,
		account_no,
		subscription_name,
		subscription_creation_date,
		subscription_sttdte,
		subscription_enddte,
		subscription_cttenddte,
		main_package,
		package_type,
		product_code,
		isVendorConv,
		status_sfdc,
		sub_group,
		copies
	FROM
		subs_base
),

-- hardcode the window functions so table can be used by later queries
subs_base_ordered AS (
	SELECT *
	FROM
		subs_base_ordered_raw
	ORDER BY
		product_code_group_1,
		account_no,
		subscription_creation_date,
		subscription_sttdte,
		subscription_enddte,
		sub_order,
		sub_dense_order_creation,
		sub_dense_order_creation_start

),

--- subscription base end ---

-- SUB EXTRA DIMENSION JOINS (BELOW) --

account_type_table AS (
	SELECT account_no, acct_record_type
	FROM cir_ss_customer
	WHERE drv_update_no = 0
),

product_grouping AS (
	SELECT
		product_code,
		product_code_group_1,
		product_code_group_2,
		prooduct_format AS product_format,
		is_key_publication
	FROM cir_ss_product_group
),

-- Contains account record type from cir_ss_customer and product groupings from cir_ss_product_group (synced google sheet) using subscription_name as key
sub_extra_dimensions AS (
	SELECT DISTINCT

		t1.subscription_name,
		t2.acct_record_type,
		t3.product_code,
		t3.product_code_group_1,
		t3.product_code_group_2,
		(CASE
			WHEN t1.product_code IN ('STT','ZBT1','BHT','SMO','WBO') AND t1.main_package ILIKE '%news tablet%' THEN 'News Tablet'
		    WHEN t1.product_code IN ('SMO','WBO') AND t1.main_package ILIKE '%all digital%' THEN 'All Digital'
		    WHEN t1.product_code IN ('STT','ZBT1','BHT','SMO','WBO') THEN '1 Digital'
			ELSE t3.product_format
			END)

		AS product_format,
		t3.is_key_publication,
		t1.sub_group,
		t1.copies
	FROM
		subs_base_ordered AS t1
	JOIN
		account_type_table AS t2
	ON
		t1.account_no = t2.account_no
	LEFT JOIN
		product_grouping AS t3
	ON
		t1.product_code = t3.product_code
),

--- SUB EXTRA DIMENSION JOINS (ABOVE) ---

--- Get list of months starting from 2010-01-01 (Earliest start year in cir_ss_subscription data) ---

month_list AS (
	SELECT
		DATE(DATE_ADD('month', 1-i, DATE_TRUNC('month', CURRENT_DATE))) AS active_month
	FROM (
		SELECT ROW_NUMBER() OVER() i
		FROM cir_ss_customer
		LIMIT 300
		) i
	WHERE active_month >= '2010-01-01'
	ORDER BY 1
),

active_subs_monthly_base AS (
	SELECT
		t2.active_month,
		t1.subscription_name,
		t1.subscription_creation_date,
		t1.subscription_sttdte,
		t1.subscription_enddte,
		t1.subscription_cttenddte,
		t1.main_package,
		t1.package_type,
		t1.product_code,
		t1.isVendorConv,
		t1.status_sfdc,
		t1.sub_group,
		t1.copies,
		'SPH Circulation' AS subscription_source,
		1 AS subscription_count
	FROM
		subs_base AS t1
	JOIN
		month_list AS t2
	ON
											-- Last day of the active month
		t1.subscription_sttdte <= DATE(DATEADD('day', -1, DATEADD('month', 1, t2.active_month)))
		AND (t1.subscription_enddte >= t2.active_month OR t1.subscription_enddte IS NULL)
),

cds_and_external_base AS (
	SELECT
		TO_DATE(subscription_month, 'Mon-YY') AS active_month,
	    'External Digital' AS acct_record_type,
		'External Digital' AS subscription_name,
		NULL AS subscription_creation_date,
		NULL AS subscription_sttdte,
		NULL AS subscription_enddte,
		NULL AS subscription_cttenddte,
		'External Digital' AS main_package,
		NULL AS package_type,
		product_code,
		FALSE AS isVendorConv,
		'External Digital' AS status_sfdc,
		'External Digital' AS subscription_source,
		subscription_count,
	    0 AS copies_count
	FROM
		cir_ss_external_subscription_source

    UNION ALL

    SELECT
		TO_DATE(month, 'Mon-YY') AS active_month,
        account_sub_type AS acct_record_type,
		'CDS & External Digital Copies' AS subscription_name,
		NULL AS subscription_creation_date,
		NULL AS subscription_sttdte,
		NULL AS subscription_enddte,
		NULL AS subscription_cttenddte,
		'CDS & External Digital Copies'  AS main_package,
		NULL AS package_type,
		product_code,
		FALSE AS isVendorConv,
		'CDS & External Digital Copies' AS status_sfdc,
		'CDS & External Digital Copies' AS subscription_source,
		0 AS subscription_count,
        copies AS copies_count
	FROM
		--pending_table


),

active_subs_monthly AS (
	SELECT
		t1.active_month,
		t1.subscription_name,
		t1.subscription_creation_date,
		t1.subscription_sttdte,
		t1.subscription_enddte,
		t1.subscription_cttenddte,
		t1.main_package,
		t1.package_type,
		t1.product_code,
		t1.isVendorConv,
		t1.status_sfdc,
		t1.subscription_source,
		t1.subscription_count,
		t1.sub_group,
		t1.copies,
		t2.acct_record_type,
		t2.product_code_group_1,
		t2.product_code_group_2,
		t2.product_format,
		t2.is_key_publication
	FROM
		active_subs_monthly_base AS t1
	LEFT JOIN
		sub_extra_dimensions AS t2
	ON
		t1.subscription_name = t2.subscription_name
),

active_subs_monthly_cdsext AS (
	SELECT
		t1.active_month,
	    t1.acct_record_type,
		t1.subscription_name,
		t1.subscription_creation_date,
		t1.subscription_sttdte,
		t1.subscription_enddte,
		t1.subscription_cttenddte,
		t1.main_package,
		t1.package_type,
		t1.product_code,
		t1.isVendorConv,
		t1.status_sfdc,
		t1.subscription_source,
		t1.subscription_count,
		NULL AS sub_group,
		NULL AS copies,
		t2.product_code_group_1,
		t2.product_code_group_2,
		'1 Digital' AS product_format,
		t2.is_key_publication
	FROM
		cds_and_external_base AS t1
	LEFT JOIN
		product_grouping AS t2
	ON
		t1.product_code = t2.product_code
),

combined_active_base AS (
	SELECT * FROM active_subs_monthly
	UNION ALL
	SELECT * FROM active_subs_monthly_cdsext
),

combined_active_base_agg AS (
	SELECT
		active_month,
		subscription_source,
		acct_record_type,
		product_code,
		product_code_group_1,
		product_code_group_2,
		product_format,
		is_key_publication,
		main_package,
		sub_group,
		sum(subscription_count) AS subscription_count,
		sum(copies) AS copies_count
	FROM
		combined_active_base
	GROUP BY
		active_month,
		subscription_source,
		acct_record_type,
		product_code,
		product_code_group_1,
		product_code_group_2,
		product_format,
		is_key_publication,
		main_package,
		sub_group
	ORDER BY 1,2,3,4,5,6,7,8,9,10
)

SELECT *,
       acct_record_type AS "Account Sub Type",
           CASE
               WHEN acct_record_type IN ('Direct Individual', 'Direct SPH Inter-Division', 'External Digital', 'ePaper')
                   THEN 'Consumer'
               WHEN acct_record_type in ('Direct Corporate', 'Airlines', 'Barter', 'Bulk Sales', 'MOE', 'Other Overseas Sales')
                   THEN 'Corporate Sub'
               END
        AS "Account Type"

    FROM combined_active_base_agg