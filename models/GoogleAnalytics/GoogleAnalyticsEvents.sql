{% if var('GoogleAnalyticsEvents') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% if var('currency_conversion_flag') %}
--depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(event_timestamp) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%googleanalytics_%events%')}} 
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}


{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% set platform_name =i.split('.')[2].split('_')[var('platform_name_position')] %}

    SELECT * {{exclude()}} (row_num)
    From (
        select
        '{{brand}}' as brand,
        '{{store}}' as store, 
        '{{platform_name}}' as platform_name,
        event_date,	
        event_timestamp,
        event_name,
        event_params,
        event_previous_timestamp,	
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info,
        user_properties,	
        user_first_touch_timestamp,
        user_ltv,	
        device.category,	
        geo,	
        app_info,
        traffic_source.source,
        traffic_source.medium,
        traffic_source.name,
        stream_id,
        platform,	
        event_dimensions,
        ecommerce.total_item_quantity,	
        ecommerce.purchase_revenue_in_usd,
        ecommerce.purchase_revenue,
        ecommerce.refund_value_in_usd,
        ecommerce.refund_value,
        ecommerce.shipping_value_in_usd,
        ecommerce.shipping_value,
        ecommerce.tax_value_in_usd,
        ecommerce.tax_value,
        ecommerce.unique_items,	
        coalesce(ecommerce.transaction_id,'') transaction_id,	
        items,	
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY event_timestamp, event_name, traffic_source.source, user_pseudo_id, ecommerce.transaction_id, device.category) row_num
	    from {{i}} a
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        WHERE a.event_timestamp  >= {{max_loaded}}
        {% endif %}) 
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}