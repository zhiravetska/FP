The idea of the project is to estimate the quota of renewable energy production in the whole consumption of energy and electricity production for 40 countires of European area. 

The statistical data have been taken from Eurostat data base:
    https://ec.europa.eu/eurostat/databrowser/view/NRG_BAL_S/default/table?lang=en&category=nrg.nrg_quant.nrg_quanta.nrg_bal - for the total consumption of energy 
    https://ec.europa.eu/eurostat/databrowser/view/NRG_INF_EPCRW/default/table?lang=en&category=nrg.nrg_quant.nrg_quanta.nrg_inf - for the energy obtained form renewable sources and biofuels.

In the dataset the data were presented for the period from 1990 till 2020. Year 2020 has been taken for the achievement of the basic goal of the project. 
Measurement unit of all the data to be analysed is GWh. 

The idea of the project is realised by means of two files: eurostat_dataset.py and Eurostat.ipynb. The first code is an obtaining of the data from Eurostat web. The second is the processing of the obtained data and its visulation.

Visualisation is represented in the barplot as the analysis of the renewables quota in the total energy consumption of th ecountries and plots representation of the persentage ratio of elecricity production to total energy and energy consumption per capita (in this case in MWh).
