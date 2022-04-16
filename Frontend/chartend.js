getChartData() // Get Data from API

generateChart() //Generate Charts in HTML

var uniqueYears;
var uniqueDev;
async function getCountrSales(data, countryName) {
    var countrySale = []
    for (var i = 0; i < data.length; i++) {
        var temp = {
            sale: data[i][`${countryName}`],
            gameName: data[i].Name
        }
        countrySale.push(temp)
    }
    return countrySale;
}


async function generateSomeDevData(nameDev, someDevShiz) {
  var data = [];
  for(let i = 0 ; i < someDevShiz.length; i++) {

    if(nameDev  == someDevShiz[i].devName) {
    
  let count = 0;
    for(let j = 0; j < year.length; j++) {
      
        if(someDevShiz[i].year == year[j]) {
          count++
          if(count == 1) {
            
            data.push(someDevShiz[i].globalSalebyYear)
          }else {
            console.log(count)
            // skip
          }
          break;
        } 
    }
  }
  }
  return data;
}

async function getChartData() {

    // const years = [];
    var highestCritcGameArr = [];
    var naSale = []
    var palSale = []
    var japSale = []
    var topYearGame = []
    var devSaleByYear = []
    // console.log("x="+x+"y="+y)
    await fetch(`http://localhost:3000/users`, {})
        .then(res => res.json())
        .then(async(response) => {

            naSale = await getCountrSales(response, 'NA_Sales')
            japSale = await getCountrSales(response, 'JP_Sales')
            palSale = await getCountrSales(response, 'PAL_Sales')
            // highest crit score
            const highestCritByGame = []

            for (var i = 0; i < response.length; i++) {
                var temp = {
                    i: i,
                    gameName: response[i].Name,
                    crtiScore: response[i].Critic_Score
                }
                highestCritByGame.push(temp)

            }

            var yearMan = [
                2001,
                2002,
                2003,
                2004,
                2005,
                2006,
                2007,
                2008,
                2009,
                2010,
                2011,
                2012,
                2013,
                2014,
                2015,
                2017,
                2018
            ]

            var devName = []
            for (let i = 0; i < response.length; i++) {
              if(response[i].Critic_Score > 9.6)
                devName.push(response[i].Developer)
            }
            uniqueDev = [... new Set(devName)]
            
            for(let i = 9; i < yearMan.length;i++) {
              for(let j = 0; j < response.length; j++) {
                for(let k = 0; k< uniqueDev.length; k++) {
                  if(uniqueDev[k] == response[j].Developer && response[j].Year == yearMan[i]) {
                    var temp = {
                      devName: response[j].Developer,
                      globalSalebyYear: response[j].Global_Sales,
                      year: response[j].Year
                    }
                    devSaleByYear.push(temp)
                  } 
                }
              }
            }
            console.log(uniqueDev)

            for (var i = 0; i < yearMan.length; i++) {
                for (var j = 0; j < response.length; j++) {
                    if (response[j].Year == yearMan[i]) {
                        if (response[j].Rank < 200) {
                            var temp = {
                                globalSales: response[j].Global_Sales,
                                rank: response[j].Rank,
                                year: response[j].Year,
                                gameName: response[j].Name
                            }
                            topYearGame.push(temp)
                        }
                    }
                }
            }

            console.log(topYearGame)

            for (let i = 0; i < highestCritByGame.length; i++) {
                if (highestCritByGame[i].crtiScore > 9.5) {
                    var temp = {
                        criticScore: highestCritByGame[i].crtiScore,
                        gameName: highestCritByGame[i].gameName
                    }
                    highestCritcGameArr.push(temp)
                }
            }
            var critScoreArr = []
            var gameName = []

            for (let i = 0; i < highestCritcGameArr.length; i++) {
                critScoreArr.push(highestCritcGameArr[i].criticScore)
                gameName.push(highestCritcGameArr[i].gameName)
            }
            // var forSalecritScoregameName = gameName.slice(0,20)
        })

    const obj = {
        uniqueYears: uniqueYears,
        highestCrit: highestCritcGameArr,
        NA_SALE: naSale,
        JAP_SALE: japSale,
        PAL_SALE: palSale,
        yearRank: topYearGame,
        devSaleByYear: devSaleByYear
    }

    return obj
}

async function generateChart() {

    var ctx = document
        .getElementById('myChart1')
        .getContext('2d');
    const score = (await getChartData()).highestCrit

    var critScoreArr = []
    var gameName = []
    for (let i = 0; i < score.length; i++) {
        critScoreArr.push(score[i].criticScore)
        gameName.push(score[i].gameName)
    }
    crittop10 = critScoreArr.sort()

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: gameName,
            datasets: [
                {
                    label: 'Critic Score for Game',
                    data: crittop10,
                    backgroundColor: [
                        'rgba(255, 99, 132, 0.2)',
                        'rgba(54, 162, 235, 0.2)',
                        'rgba(255, 206, 86, 0.2)',
                        'rgba(75, 192, 192, 0.2)',
                        'rgba(153, 102, 255, 0.2)',
                        'rgba(255, 159, 64, 0.2)'
                    ],
                    borderColor: [
                        'rgba(255,99,132,1)',
                        'rgba(54, 162, 235, 1)',
                        'rgba(255, 206, 86, 1)',
                        'rgba(75, 192, 192, 1)',
                        'rgba(153, 102, 255, 1)',
                        'rgba(255, 159, 64, 1)'
                    ],
                    borderWidth: 4
                }
            ]
        },
        options: {
            scales: {
                yAxes: [
                    {
                        ticks: {
                            min: 90
                        }
                    }
                ]
            }
        }
    });

    var ctx = document
        .getElementById('myChart0.1')
        .getContext('2d');
    const naSale = (await getChartData()).NA_SALE

    var saleNA = []
    var gameNameNA = []

    for (let i = 0; i < naSale.length; i++) {
        if (naSale[i].sale > 10) {
            saleNA.push(naSale[i].sale)

            gameNameNA.push(naSale[i].gameName)
        }
    }
    console.log(saleNA)

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: gameNameNA,
            datasets: [
                {
                    label: 'Sales in NA',
                    data: saleNA.sort(),
                    backgroundColor: [
                        'rgba(255, 99, 132, 0.2)',
                        'rgba(54, 162, 235, 0.2)',
                        'rgba(255, 206, 86, 0.2)',
                        'rgba(75, 192, 192, 0.2)',
                        'rgba(153, 102, 255, 0.2)',
                        'rgba(255, 159, 64, 0.2)'
                    ],
                    borderColor: [
                        'rgba(255,99,132,1)',
                        'rgba(54, 162, 235, 1)',
                        'rgba(255, 206, 86, 1)',
                        'rgba(75, 192, 192, 1)',
                        'rgba(153, 102, 255, 1)',
                        'rgba(255, 159, 64, 1)'
                    ],
                    borderWidth: 4
                }
            ]
        },
        options: {
            scales: {
                yAxes: [
                    {
                        ticks: {
                            min: 90
                        }
                    }
                ]
            }
        }
    });

    var ctx = document
        .getElementById('myChart0.2')
        .getContext('2d');
    const japSale = (await getChartData()).JAP_SALE

    var saleJAP = []
    var gameNameJAP = []

    for (let i = 0; i < japSale.length; i++) {
        if (japSale[i].sale > 4) {
            saleJAP.push(japSale[i].sale)
            gameNameJAP.push(japSale[i].gameName)
        }
    }

    console.log(saleJAP.length)
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: gameNameJAP,
            datasets: [
                {
                    label: 'Sales in JAP',
                    data: saleJAP.sort(),
                    backgroundColor: [
                        'rgba(255, 99, 132, 0.2)',
                        'rgba(54, 162, 235, 0.2)',
                        'rgba(255, 206, 86, 0.2)',
                        'rgba(75, 192, 192, 0.2)',
                        'rgba(153, 102, 255, 0.2)',
                        'rgba(255, 159, 64, 0.2)'
                    ],
                    borderColor: [
                        'rgba(255,99,132,1)',
                        'rgba(54, 162, 235, 1)',
                        'rgba(255, 206, 86, 1)',
                        'rgba(75, 192, 192, 1)',
                        'rgba(153, 102, 255, 1)',
                        'rgba(255, 159, 64, 1)'
                    ],
                    borderWidth: 4
                }
            ]
        },
        options: {
            scales: {
                yAxes: [
                    {
                        ticks: {
                            min: 90
                        }
                    }
                ]
            }
        }
    });


    // CHART 3

    var ctx = document
        .getElementById('myChart3')
        .getContext('2d');
    const somerankduo = (await getChartData()).yearRank

    var rank = [];
    var year = [];
    var globalSale = [];
    var gameName = []
    for (let i = 0; i < somerankduo.length; i++) {

        rank.push(somerankduo[i].rank)
        globalSale.push(somerankduo[i].globalSales)
        year.push(somerankduo[i].year)
        gameName.push(somerankduo[i].gameName)
    }

    const footer = (tooltipItems) => {
        for (let i = 0; i < somerankduo.length; i++) {
            if (tooltipItems[0].chart.tooltip.dataPoints[1].raw == somerankduo[i].rank) {
                return 'Game:' + somerankduo[i].gameName
            }
        }
    }

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: year,
            datasets: [
                {
                    label: 'Global Sale',
                    data: globalSale,
                    backgroundColor: [
                        'rgba(255, 99, 132, 0.2)',
                        'rgba(54, 162, 235, 0.2)',
                        'rgba(255, 206, 86, 0.2)',
                        'rgba(75, 192, 192, 0.2)',
                        'rgba(153, 102, 255, 0.2)',
                        'rgba(255, 159, 64, 0.2)'
                    ],
                    borderColor: [
                        'rgba(255,99,132,1)',
                        'rgba(54, 162, 235, 1)',
                        'rgba(255, 206, 86, 1)',
                        'rgba(75, 192, 192, 1)',
                        'rgba(153, 102, 255, 1)',
                        'rgba(255, 159, 64, 1)'
                    ],
                    stack: 'combined',
                    type: 'bar',
                    borderWidth: 4
                }, {
                    label: 'Rank',
                    data: rank,
                    backgroundColor: 'rgba(255, 159, 64, 1)',
                    stack: 'combined'
                }
            ]
        },
        options: {
            interaction: {
                intersect: false,
                mode: 'index'
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        footer: footer
                    }
                }
            }
        }
    });


  // CHART 4

  var ctx = document
  .getElementById('myChart4')
  .getContext('2d');
const someDevShiz = (await getChartData()).devSaleByYear

console.log(someDevShiz)

var rank = [];
var year = [2010, 2011, 2012, 2013, 2014, 2015, 2017, 2018];
var globalSale = [];
var gameName = []

// const footer = (tooltipItems) => {
//   for (let i = 0; i < somerankduo.length; i++) {
//       if (tooltipItems[0].chart.tooltip.dataPoints[1].raw == somerankduo[i].rank) {
//           return 'Game:' + somerankduo[i].gameName
//       }
//   }
// }
// devName
// globalSalebyYear
// year



new Chart(ctx, {
  type: 'radar',
  data: {
      labels: [2010, 2011, 2012, 2013, 2014, 2015, 2017, 2018],
         datasets: [
          {
            label: 'Nintendo EPD',
            data: [54, 65, 60, 70, 70, 75, 68.87, 57.5],
            borderColor: 'rgba(255, 99, 132, 0.2)',
            backgroundColor: 'rgba(255, 99, 132)',
            fill: '-1'
          },
          {
            label: 'Nintendo EAD Tokyo',
            data:  [24, 25.6, 30.1, 10, 30, 55, 34.56, 17.87],
            borderColor: 'rgba(54, 162, 235, 0.2)',
            backgroundColor: 'rgba(54, 162, 235, 0.2)',
            fill: '-1'
          },
          {
            label: 'SIE Santa Monica Studio',
            data:   [44, 76, 78, 13, 43, 10, 23.23],
            borderColor: 'rgba(255, 206, 86, 0.2)',
            backgroundColor: 'rgba(255, 206, 86, 0.2)',
            fill: '-1'
          },
          {
            label: 'Nintendo EAD Tokyo',
            data:   [20, 30, 40, 80, 20, 80, 12.6, 34.6],
            borderColor: 'rgba(75, 192, 192, 0.2)',
            fill: '-1'
          },
          {
            label: 'Rockstar North',
            data:  [25.48,54.16,7.61,8.06,4.45, 65, 12, 14.54],
            borderColor: 'rgba(75, 192, 192, 0.2)',
            fill: '-1'
          },
          {
            label: 'Rare Ltd.',
            data:  [8.77,55.61,21.69,6.62,6.82, 3.47, 2,89],
            borderColor: 'rgba(75, 192, 132, 0.2)',
            fill: '-1'
          }
          // {
          //   label: 'Rockstar Games',
          //   data: generateSomeDevData('Rockstar Games', someDevShiz),
          //   borderColor: 'red',
          //   backgroundColor: 'red',
          // },
          // {
          //   label: 'Valve Software',
          //   data: generateSomeDevData('Valve Software', someDevShiz),
          //   borderColor: 'red',
          //   backgroundColor: 'red',
          // },
          // {
          //   label: 'SquareSoft',
          //   data: generateSomeDevData('SquareSoft', someDevShiz),
          //   borderColor: 'red',
          //   backgroundColor: 'red',
          // },
          // {
          //   label: 'Square',
          //   data: generateSomeDevData('Square', someDevShiz),
          //   borderColor: 'red',
          //   backgroundColor: 'red',
          // },
          // {
          //   label: 'Capcom',
          //   data: generateSomeDevData('Capcom', someDevShiz),
          //   borderColor: 'red',
          //   backgroundColor: 'red',
          // },
          // {
          //   label: 'Rockstar San Diego',
          //   data: generateSomeDevData('Rockstar San Diego', someDevShiz),
          //   borderColor: 'red',
          //   backgroundColor: 'red',
          // },
          
      ]
  },
  options: {
      interaction: {
          intersect: false,
          mode: 'index'
      },
      plugins: {
          tooltip: {
              callbacks: {
                  footer: footer
              }
          }
      }
  }
});

}
