from dataclasses import dataclass


@dataclass
class Bratislava_constants:
    household_ykwh_average = 2500
    households = 1
    cars = 0
    km_driven = 0
    cars_per_household = 2.0
    diesel_cars = 1  # % of cars that use diesel
    zse_co2 = 0.142  # kg/kWh
    solar_co2 = 0.041  # kg/kWh
    electric_km = 0.195  # kWh/km
    diesel_co2 = 0.208  # kg/km


@dataclass
class Emission_savings_calculator:
    panel_ykwh_production: float
    constants = Bratislava_constants()

    def diesel_car_consumption(self, population_treshold: float):
        car_count = self.constants.households * self.constants.cars_per_household * self.constants.diesel_cars * population_treshold
        kilometers = car_count * self.constants.km_driven
        return kilometers * self.constants.diesel_co2

    def electro_from_solar(self, population_treshold: float, solar_coverage: float = 0.5):
        car_count = self.constants.households * self.constants.cars_per_household * self.constants.diesel_cars * population_treshold
        kilometers = car_count * self.constants.km_driven
        return kilometers * self.constants.solar_co2 * self.constants.electric_km * solar_coverage

    def electro_from_zse(self, population_treshold: float, zse_coverage: float = 0.5):
        car_count = self.constants.households * self.constants.cars_per_household * self.constants.diesel_cars * population_treshold
        kilometers = car_count * self.constants.km_driven
        return kilometers * self.constants.zse_co2 * self.constants.electric_km * zse_coverage

    def car_co2_savings(self, population_treshold: float, solar_coverage: float = 0.5):
        return self.diesel_car_consumption(population_treshold) - self.electro_from_solar(population_treshold, solar_coverage) - self.electro_from_zse(population_treshold, 1-solar_coverage)

    def household_co2_savings(self, population_treshold: float):
        solar_coverage = 0
        if self.panel_ykwh_production > self.constants.household_ykwh_average:
            solar_coverage = 1
        else:
            solar_coverage = self.panel_ykwh_production / self.constants.household_ykwh_average

        return self.constants.households * self.constants.household_ykwh_average * solar_coverage * (self.constants.zse_co2 - self.constants.solar_co2) * population_treshold
    

if __name__ == "__main__":
    # calc = Emission_savings_calculator(panel_ykwh_production=3160*2)

    # print(calc.household_co2_savings(0.15))
    # print(calc.household_co2_savings(0.30))
    # print(calc.household_co2_savings(0.90))

    # print(calc.car_co2_savings(0.15, 0.5))
    # print(calc.car_co2_savings(0.30, 0.5))
    # print(calc.car_co2_savings(0.90, 0.5))

    calc_personal = Emission_savings_calculator(panel_ykwh_production=3179)
    print(calc_personal.household_co2_savings(population_treshold=1))
