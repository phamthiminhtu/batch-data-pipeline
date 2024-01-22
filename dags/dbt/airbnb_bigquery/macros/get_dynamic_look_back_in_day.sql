{# model_params = namespace containing at least ds and ds_date and lookback_in_days #}
{%- macro get_dynamic_look_back_in_day(model_params, hour_with_special_look_back_day=20, special_day_interval=3, default_day_interval=3) -%}
    {% if model_params.ds_date.hour == hour_with_special_look_back_day %}
        {% set model_params.lookback_in_days = special_day_interval %}
    {% else %}
        {% set model_params.lookback_in_days = default_day_interval %}
    {% endif %}
{%- endmacro -%}
