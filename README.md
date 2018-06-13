# biketour - plan your bike trip

This tool allows to choose a perfect day to go for a long bike trip.

## What is this all about?

Imagine you want to make to long cycle trip in one day. Say, you want
to cover over 200 miles by travelling, say, from Aachen to Calais.

As an experienced cyclist, you know that there are several obstacles,
that can harden your journey: climbs and wind. You cannot do anything
about climbs (luckily, from Aachen to Calais there are non), but wind
can be a real trouble-maker. Especially, heading West in West Europe,
strong wind from Atlantic can slower you down.

Can we compute how bad the actual wind and weather situation is? This
tool is designed precisely for that job.

## Physical model

Currently, the physical model being used in calculations is borrowed
from [the bicycle performance Wikipedia
page](https://en.wikipedia.org/wiki/Bicycle_performance). The model is
used to compute the speed of a cycle at any point along the route
given a rider cycle power input. The computed speed at any time point
along the route allows to compute a time plan for the trip.

The following physical parameters are used in the model:

 + input power of the rider in Watts. Currently, the model assumes a
   constant power input along the journey. This can be easily
   generalised to a other power input models (unfortunately, I don't
   have a powermeter and have no idea how rider power changes along
   the journey).

 + total mass of the rider in kg

 + wind speed and wind bearing. The wind speed and direction is used
   to compute power needed to overcome the aerodynamic force.

   Here is used a simple model, assuming a fixed drag coefficient. In
   reality, the drag coefficient should depend on wind
   direction. There are plans to extend the model that takes it into
   account.

 + drag coefficient. A parameter that hard to measure physically. As
   said above, the parameter is assumed to be fixed.

 + air pressure, temperature and relative humidity. Those parameters
   are used to compute the air density, that affects the power to
   overcome the air drag.

 + rolling resistance coefficient. This parameter is affected by your
   tires, air pressure in them, the wheel weight and the quality of
   the bearings you have.

 + drivetrain efficiency. This parameter describes how efficient your
   drivetrain. Assuming to be fixed along the trip.

## Weather data source

For computing the trip time plan, we need to know the weather
conditions at each point along the route. Fortunately, there is a good
service that can provide us with that sort of data.

The service is called [Dark Sky](https://darksky.net/poweredby/) and
data is queried from them. To make if work you need to get an API-key.

The service is not free, but it is quiet cheap. I queried historical
weather data for 500 different days in the last 30 years for the route
from Aachen to Calais in ~20000 query requests. That costs ~2 EUR.

## Accuracy of the model; those silly physical parameters

It is not claim that model is gives accurate estimates for your
journey longevity. But it should correlate with the true values.

The accuracy of the model depends on the weather data accuracy and the
accuracy of the physical parameters you have provided.

It is planned to have a feature that estimates your physical
parameters given a real journey trip (a data with coordinates,
elevation and input power).

## The route coordinates

The cycling routes are generated by [cycle
travel](http://cycle.travel/map) service.

## Application example: Aachen -> Calais

The route from Aachen to Calais is approximately 228 miles. The
queried historical weather data allows to compute the travel time
plans. The trip longevity distribution is shown in the figure below.

![distribution of time required for the route](docs/aix-calais.png?raw=true)

## Suggestions and critic

Suggestions, critic (raise an issue) and pull requests are welcome!

## ToDO features:

 + [DONE] query historical weather database

   for a given route create and populate database with weather
   historical records along the journey

 + [DONE] physical model based on a constant power output

   Implement a physical model that assumes that a cyclist cycles with
   a constant power. This model takes into account wind, temperature,
   pressure, humidity, climbs, efficiency of drive_train, etc

 + [DONE] compute a trip plan

   The trip plan is computed using a physical model

 + [DONE] compute plan characteristics for different data

 + estimate physical model parameters using the travel data

   yet to be implemented: the travel data should include power-meter
   measurements as well as gps coordinates, time and elevation

 + compute how good the next days forecast for taking the journey

 + include regular pauses in a ride plan

 + graphs, plots


## Attribution

The weather data is [Powered by Dark
Sky](https://darksky.net/poweredby/).

The cycling routes are generated by
[cycle.travel](http://cycle.travel/map).
