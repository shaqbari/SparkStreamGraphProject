<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE>
<html >
<head>
<meta charset="UTF-8">
<title>Insert title here</title>
<link rel="stylesheet" href="css/style.css">
<script src="https://www.amcharts.com/lib/3/amcharts.js"></script>
<script src="https://www.amcharts.com/lib/3/serial.js"></script>
<script>

</script>
</head>
<div id="chartdiv"></div>
<!--조절바 <div class="container-fluid">
  <div class="row text-center" style="overflow:hidden;">
    <div class="col-sm-3" style="float: none !important;display: inline-block;">
      <label class="text-left">Top Radius:</label>
      <input class="chart-input" data-property="topRadius" type="range" min="0" max="1.5" value="1" step="0.01" />
    </div>

    <div class="col-sm-3" style="float: none !important;display: inline-block;">
      <label class="text-left">Angle:</label>
      <input class="chart-input" data-property="angle" type="range" min="0" max="89" value="30" step="1" />
    </div>

    <div class="col-sm-3" style="float: none !important;display: inline-block;">
      <label class="text-left">Depth:</label>
      <input class="chart-input" data-property="depth3D" type="range" min="1" max="120" value="40" step="1" />
    </div>
  </div>
</div> -->
  <script src='https://code.jquery.com/jquery-1.11.2.min.js'></script>

    <!-- <script src="js/index.js"></script> -->
    <script>
    var chart = AmCharts.makeChart("chartdiv", {
  	  "type": "serial",
  	  "startDuration": 2,
  	  "dataProvider": <%=request.getAttribute("json")%>,
  	  "valueAxes": [{
  	    "position": "left",
  	    "axisAlpha": 0,
  	    "gridAlpha": 0
  	  }],
  	  "graphs": [{
  	    "balloonText": "[[category]]: <b>[[value]]</b>",
  	    "colorField": "color",
  	    "fillAlphas": 0.85,
  	    "lineAlpha": 0.1,
  	    "type": "column",
  	    "topRadius": 1,
  	    "valueField": "visits"
  	  }],
  	  "depth3D": 40,
  	  "angle": 30,
  	  "chartCursor": {
  	    "categoryBalloonEnabled": false,
  	    "cursorAlpha": 0,
  	    "zoomable": false
  	  },
  	  "categoryField": "country",
  	  "categoryAxis": {
  	    "gridPosition": "start",
  	    "axisAlpha": 0,
  	    "gridAlpha": 0

  	  },
  	  "exportConfig": {
  	    "menuTop": "20px",
  	    "menuRight": "20px",
  	    "menuItems": [{
  	      "icon": '/lib/3/images/export.png',
  	      "format": 'png'
  	    }]
  	  }
  	}, 0);

  	jQuery('.chart-input').off().on('input change', function() {
  	  var property = jQuery(this).data('property');
  	  var target = chart;
  	  chart.startDuration = 0;

  	  if (property == 'topRadius') {
  	    target = chart.graphs[0];
  	  }

  	  target[property] = this.value;
  	  chart.validateNow();
  	});
    </script>
    
</html>