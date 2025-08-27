-- 2311 rows returned
SELECT
    *
FROM dvdrental.public.rental
LEFT JOIN dvdrental.public.customer ON
    rental.customer_id = customer.customer_id

LEFT JOIN dvdrental.public.address ON
    customer.address_id = address.address_id

LEFT JOIN dvdrental.public.city ON
    address.city_id = city.city_id

LEFT JOIN dvdrental.public.payment ON
    rental.rental_id = payment.rental_id

LEFT JOIN dvdrental.public.inventory ON
    rental.inventory_id = inventory.inventory_id

LEFT JOIN dvdrental.public.film ON
    inventory.film_id = film.film_id

LEFT JOIN dvdrental.public.film_category ON
    film.film_id = film_category.film_id

LEFT JOIN dvdrental.public.category ON
    film_category.category_id = category.category_id

WHERE 1=1
AND rental.rental_date BETWEEN '2005-06-14' AND '2005-06-22'
;