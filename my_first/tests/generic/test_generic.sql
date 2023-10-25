{% test col_greater_than(model, column_name, value) %}

    SELECT
	   {{ column_name }} AS row_that_failed
	FROM {{ model }}
	WHERE NOT {{ column_name }} <= {{ value }}

{% endtest %}


{% test col_less_than(model, column_name, value) %}

    SELECT
	   {{ column_name }} AS row_that_failed
	FROM {{ model }}
	WHERE NOT {{ column_name }} > {{ value }}

{% endtest %}