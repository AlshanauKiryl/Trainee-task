---Display the number of films in each category, sorted in descending order.
select film_category.name,count(film_id) as film_count
from film_category
group by film_category.name
order by film_count desc;

--Display the top 10 actors whose films were rented the most, sorted in descending order.
select actor.first_name, actor.last_name, count(rental.rental_id) AS rental_count
from actor
join film_actor on actor.actor_id = film_actor.actor_id
join film on film.film_id = film_actor.film_id
join inventory on film_actor.film_id = inventory.film_id
join rental on rental.inventory_id = inventory.inventory_id
group by actor.actor_id,actor.first_name, actor.last_name
order by rental_count desc
limit 10;

--Display the category of films that generated the highest revenue.

select category.name, sum(payment.amount) as total_payment
from payment
join rental on payment.rental_id = rental.rental_id
join inventory on rental.inventory_id = inventory.inventory_id
join film_category on inventory.film_id = film_category.film_id
join category on film_category.category_id = category.category_id
group by category.name
order by total_payment desc;

--Display the titles of films not present in the inventory. Write the query without using the IN operator.
select film.title,inventory.inventory_id
from film
left join inventory on film.film_id = inventory.film_id
where inventory.film_id is null;

--Display the top 3 actors who appeared the most in films within the "Children" category.
--If multiple actors have the same count, include all
with ActorFilmCounts as (select actor.first_name, actor.last_name, count(actor.actor_id) as appear_count
                         from film_category
                                  join category on film_category.category_id = category.category_id
                                  join film_actor on film_category.film_id = film_actor.film_id
                                  join actor on film_actor.actor_id = actor.actor_id
                         where category.name = 'Children'
                         group by actor.actor_id, actor.first_name, actor.last_name)

select first_name, last_name, appear_count
from (
    select first_name, last_name,appear_count,rank() over (order by appear_count desc) as actor_rank
    from ActorFilmCounts) as RankedActors
    where actor_rank <= 3;

--Display cities with the count of active and inactive customers (active = 1).
--Sort by the count of inactive customers in descending order.

select city.city, count(*) filter (where customer.active = 1) as active_count,
                count(*) filter (where customer.active = 0) as inactive_count
from customer
join address on customer.address_id = address.address_id
join city on address.city_id = city.city_id
group by city.city
order by inactive_count desc;

--Display the film category with the highest total rental hours in cities
--where customer.address_id belongs to that city and starts with the letter "a".
--Do the same for cities containing the symbol "-". Write this in a single query.

select
    city_condition,category_name,total_rental_hours
from (
    select
        city_condition,
        category_name,
        total_rental_hours,
        rank() over (partition by city_condition order by total_rental_hours desc ) as category_rank
    from (
        select
            category.name as category_name,
            case
                when city.city ilike 'a%' then 'City names starts with "A"'
                when city.city like '%-%' then 'City names starts with "-"'
            end as city_condition,
            sum(extract(epoch from (rental.return_date - rental.rental_date)) / 3600) as total_rental_hours
        from rental
        join customer on rental.customer_id = customer.customer_id
        join address on customer.address_id = address.address_id
        join city ON address.city_id = city.city_id
        join inventory ON rental.inventory_id = inventory.inventory_id
        join film ON inventory.film_id = film.film_id
        join film_category ON film.film_id = film_category.film_id
        join category ON film_category.category_id = category.category_id
        where
            (city.city ilike 'a%' or city.city like '%-%') and rental.return_date is not null
        group by
            city_condition, category.name
    ) as aggregated_data
) as ranked_data
where
    category_rank = 1;