-- 2311 records returned
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

-- 15_866 records returned
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

FROM dvdrental.public.rental AS rental
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
AND rental.rental_date BETWEEN '2005-05-24' AND '2005-08-24'
;

SELECT MIN(rental_date), MAX(rental_date) FROM dvdrental.public.rental
;

SELECT DISTINCT(DATE(rental_date)), COUNT(1) FROM dvdrental.public.rental
GROUP BY 1
ORDER BY 1
;