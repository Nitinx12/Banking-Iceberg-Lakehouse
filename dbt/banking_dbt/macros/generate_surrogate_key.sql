{% macro generate_surrogate_key(fields) %}
  md5(concat_ws('|', {% for f in fields %}coalesce(cast({{ f }} as varchar), '_null'){% if not loop.last %}, {% endif %}{% endfor %}))
{% endmacro %}
