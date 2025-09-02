SELECT
    rental.rental_id,
    rental.rental_date,
    rental.return_date,
    rental.last_update AS last_rental_update,
    rental.customer_id,
    customer.first_name,
    customer.last_name,
    customer.email,
    customer.activebool,
    customer.active,
    customer.create_date,
    customer.last_update AS last_customer_update,
    customer.address_id,
    address.address,
    address.address2,
    address.district,
    address.postal_code,
    address.phone,
    address.last_update AS last_address_update,
    address.city_id,
    city.city,
    payment.payment_id,
    payment.amount,
    payment.payment_date,
    inventory.inventory_id,
    inventory.last_update AS last_inventory_update,
    category.name AS category_name,
    film.*    

FROM {schema_name}.rental
LEFT JOIN {schema_name}.customer ON
    rental.customer_id = customer.customer_id

LEFT JOIN {schema_name}.address ON
    customer.address_id = address.address_id

LEFT JOIN {schema_name}.city ON
    address.city_id = city.city_id

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
AND rental.{partition_column} BETWEEN '{from_dt}' AND '{to_dt}'
