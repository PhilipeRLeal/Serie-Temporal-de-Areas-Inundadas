# Primeiro, importa as bibliotecas utilizadas
import ee
import datetime as dt
import pandas as pd
import fiona
import os
ee.Initialize()

# 1 - input dos dados
os.chdir(r'C:\Users\Edson\Documents\GitHub\Séries_Temporais_de_Áreas_Inundadas\Serie-Temporal-de-Areas-Inundadas') #  Dire
#tório para salvar o DataFrame
Geom = r"C:\Users\Edson\Documents\MEGAsync\ser\ser300\proj_geoprocessamento\lago.shp"  # Poligono da área a ser analisada
DateStart = (2003, 1, 1)  # Data inicial a série temporal
DateEnd = (2003, 3, 30)  # Data final da série temporal
ResTemp = 10  # Resolução temporal da série temporal (dias para fazer o mosaico)
Escala = 500  # Escala dos cálculos
Proj = 'EPSG:4326' #  Projeção para salvar e cálculas as áreas
SaveImg = True

# ------------- fim do input ----------

# 2 - Gera variaveis secundarias e funções para o processamento

#  Obtem as coordenadas e projeção do poligono, e le a área de estudo no ee
Poligono = fiona.open(Geom)
PoliProj = Poligono.crs['init']
Coord = [list(x) for x in [x['geometry']['coordinates'] for x in Poligono][0][0]]
Area = ee.Geometry.Polygon(Coord, PoliProj)
region = Area.getInfo()['coordinates']  # Define o input para salvar as imagens

#  Função para gerar a máscara de nuvem
def getqabits(image, start, end, newname):
    pattern = 0
    for i in list(range(start, end+1)):
        pattern += 2**i
        return image.select([0], [newname]).bitwiseAnd(pattern).rightShift(start)


# Data inicial e final no formato datetime
DateStartT = dt.datetime(DateStart[0], DateStart[1], DateStart[2])
DateEndT = dt.datetime(DateEnd[0], DateEnd[1], DateEnd[2])

# Gera o DataFrame para salvar os resultados da série temporal
index = pd.date_range(DateStartT, DateEndT, freq=str(ResTemp) + 'D')
header = ['Cobertura_Válida', 'Water_cover (km²)']
Data = pd.DataFrame( {}, index = index, columns = header)  # DataFrame para salvar as coberturas por dia e data

# 3 - Começo do processamento
for date in index:  # Controla a data inicial da composição
    image = ee.Image('MODIS/006/MYD09GA/' + date.isoformat()[0:10].replace('-', '_')) # Imagem inicial da composição
    NDWI = image.expression('(Verde - NIR) / (Verde + NIR)',
                            dict(NIR=image.select('sur_refl_b02'), Verde=image.select('sur_refl_b04')))

    cloud = getqabits(image.select('state_1km'), 0, 1, 'cloud_state')
    cloud = cloud.expression("b(0) == 1 || b(0) == 2")
    pixel_valido = cloud.expression("b(0) == 0")

    agua = NDWI.expression("b(0) > 0").mask(pixel_valido)

    if ResTemp > 1:
        mosaico = pd.date_range(dt.datetime(date.year, date.month, date.day), periods=ResTemp)[1:]
        for composicao in mosaico:  # controla a intesecção dos dias no mosaico.
            image2 = ee.Image('MODIS/006/MYD09GA/' + composicao.isoformat()[0:10].replace('-', '_'))  # Imagem a ser
            # adicionada no mosaico

            NDWI2 = image2.expression('(Verde - NIR) / (Verde + NIR)',
                                      dict(NIR=image2.select('sur_refl_b02'), Verde=image2.select('sur_refl_b04')))

            cloud2 = getqabits(image2.select('state_1km'), 0, 1, 'cloud_state')
            cloud2 = cloud2.expression("b(0) == 1 || b(0) == 2")
            pixel_valido2 = cloud2.expression("b(0) == 0")

            agua2 = NDWI2.expression("b(0) > 0").mask(pixel_valido2)

            agua = ee.Image([agua.select(0), agua2.select(0)])

            pixel_valido = ee.Image([pixel_valido.select(0), pixel_valido2.select(0)])

            agua = agua.reduce(ee.Reducer.max())  # obtem o valor máximo da composicao, ou seja, se apenas uma vez foi
            # inundada
            pixel_valido = pixel_valido.reduce(ee.Reducer.max())  # obtem o máximo da composicao, ou seja, se ja ocorreu
            # alguma vez pixel válido

    if Proj != False:
        agua = agua.reproject(Proj, None, Escala)
        pixel_valido = pixel_valido.reproject(Proj, None, Escala)

    cobertura_agua = agua.reduceRegion(reducer='sum', geometry = Area, scale = Escala)
    cobertura_valida = pixel_valido.reduceRegion(reducer='mean', geometry = Area, scale = Escala)

    Data.loc[date]['Cobertura_Válida'] = cobertura_valida.getInfo()['max'] * 100 #  cálculo da área de pixels válidos
    Data.loc[date]['Water_cover (km²)'] = (cobertura_agua.getInfo()['max']*(Escala**2)) / (1000**2) #  cálculo
    # da área coberta por água por composição

    if SaveImg == True:
        task_config = dict(description='Agua', region=region)
        task = ee.batch.Export.image(agua, 'Agua' + date.strftime("%Y-%m-%d"), task_config)
        task.start()

        task_config = {
            'description': 'Pixel_validos',
            'region': region
        }
        task = ee.batch.Export.image(pixel_valido, 'P_valido' + date.strftime("%Y-%m-%d"), task_config)
        task.start()

Data.to_csv('Agua' + DateStartT.strftime("%Y-%m-%d") + 'to' + DateEndT.strftime("%Y-%m-%d") + '.csv')