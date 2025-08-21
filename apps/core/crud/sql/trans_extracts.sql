SELECT
    *
FROM {schema_name}.rental
LEFT JOIN {schema_name}.customer ON
    rental.customer_id = customer.customer_id

LEFT JOIN {schema_name}.`address` ON
    customer.address_id = `address`.address_id

LEFT JOIN {schema_name}.city ON
    `address`.city_id = city.city_id

LEFT JOIN {schema_name}.payment ON
    rental.rental_id = payment.rental_id

LEFT JOIN {schema_name}.inventory ON
    rental.inventory_id = inventory.inventory_id

LEFT JOIN {schema_name}.film ON
    inventory.film_id = film.film_id

LEFT JOIN {schema_name}.film_category ON
    film.film_id = film_category.film_id

LEFT JOIN {schema_name}.category ON
    film_category.category_id = category.category_id

WHERE 1=1
AND rental.{partition_column} BETWEEN {from_dt} AND {to_dt}
