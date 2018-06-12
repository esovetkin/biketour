#!/bin/env python

class Trip_Characteristics:
    """Given a plan the class returns a list of available journey
    characteristics

    """

    def __init__(self, plan):
        """Initialise the class

        :plan: a pandas DataFrame with journey coordinates and weather

        """
        self.plan = plan

    def _compute_journey_time(self):
        """Compute journey longevity

        """
        return self.plan['time'].max()-self.plan['time'].min()

    def _compute_journey_distance(self):
        """Compute joruney distance
        
        """
    
    def _compute_wind_index(self):
        """Compute some wind quantities along the route

        """

    def _compute_climbing_index(self):
        """Compute some climbing quantitites along the route

        """

    def _compute_column_quantities(self, column):
        """Compute weather related quantities along the journey

        """
        return (self.plan[column].min(),
                self.plan[column].max(),
                self.plan[column].mean())
        
        
    def _compute_precipitation_quantities(self):
        """Compute some precipitation quantities.

        Such as overall probability and overall amount

        """
        x = self.plan[['w_time','w_precipProbability','w_precipIntensity']]\
                  .drop_duplicates(subset="w_time")

        # silly probability computation, assuming independent events
        
        return (1-(1-x.w_precipProbability).prod(min_count=1),
                x.w_precipIntensity.sum(min_count=1))

    def _compute_most_likely_summary(self):
        """Get most common summary

        """
        return self.plan.w_summary.value_counts().idxmax() 
    
    def get_all(self):
        res={}
        res["time"] = self._compute_journey_time()
        # res["distance"] = self._compute_journey_distance()
        # res["wind_index"] = self._compute_wind_index()
        # res["climbing_index"] = self._compute_climbing_index()
        
        x = self._compute_precipitation_quantities()
        res["w_precipProbability"] = x[0]
        res["w_precipIntensity"] = x[1]

        res["summary"] = self._compute_most_likely_summary()

        column_quantites = ['w_temperature','w_apparentTemperature',
                            'w_dewPoint','w_humidity','w_pressure',
                            'w_cloudCover','w_uvIndex', 'w_visibility','w_ozone']

        for column in column_quantites:
            x = self._compute_column_quantities(column)
            res[column + "_Min"] = x[0]
            res[column + "_Max"] = x[1]
            res[column + "_Average"] = x[2]
                    
        return res
