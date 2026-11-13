const totals={
  Chicago:{cheapest:[33.71,31.23],median:[62.17,59.53]},
  Dallas:{cheapest:[35.39,34.59],median:[50.38,52.69]},
  "Los Angeles":{cheapest:[33.58,33.38],median:[64.32,63.74]},
  Seattle:{cheapest:[39.95,42.65],median:[68.93,67.45]}
};

const quantities={
  "Apples":"16 oz","Tomatoes":"16 oz","Whole milk":"64 fl oz","Eggs":"12 ct",
  "Butter":"16 oz","White bread":"20 oz","Ground beef":"16 oz",
  "Chicken thighs":"16 oz","Black beans":"15 oz","Frozen broccoli":"12 oz",
  "Cereal":"12 oz","Orange juice":"52 fl oz"
};

const categoryMovers={
  Chicago:[
    {label:"Bakery",value:1.72,terms:11,products:82},
    {label:"Beverages",value:-3.59,terms:11,products:116},
    {label:"Dairy and eggs",value:-5.89,terms:13,products:199},
    {label:"Frozen",value:-7.92,terms:15,products:87},
    {label:"Meat and seafood",value:-.16,terms:12,products:141},
    {label:"Pantry",value:-4.02,terms:31,products:336},
    {label:"Produce",value:5.1,terms:13,products:63},
    {label:"Snacks and candy",value:-1.3,terms:12,products:121}
  ],
  Dallas:[
    {label:"Bakery",value:.31,terms:7,products:16},
    {label:"Beverages",value:-4.5,terms:7,products:25},
    {label:"Dairy and eggs",value:-2.93,terms:12,products:59},
    {label:"Frozen",value:.46,terms:6,products:17},
    {label:"Meat and seafood",value:.07,terms:11,products:37},
    {label:"Pantry",value:-1.17,terms:22,products:62},
    {label:"Produce",value:7.06,terms:10,products:22},
    {label:"Snacks and candy",value:-1.41,terms:9,products:15}
  ],
  "Los Angeles":[
    {label:"Bakery",value:.3,terms:10,products:68},
    {label:"Beverages",value:-.12,terms:12,products:95},
    {label:"Dairy and eggs",value:-2.12,terms:13,products:180},
    {label:"Frozen",value:-.92,terms:13,products:86},
    {label:"Meat and seafood",value:-.44,terms:13,products:136},
    {label:"Pantry",value:.7,terms:30,products:316},
    {label:"Produce",value:4.58,terms:13,products:72},
    {label:"Snacks and candy",value:8.36,terms:11,products:107}
  ],
  Seattle:[
    {label:"Bakery",value:-.99,terms:11,products:62},
    {label:"Beverages",value:-1.8,terms:11,products:127},
    {label:"Dairy and eggs",value:-.15,terms:13,products:201},
    {label:"Frozen",value:1.76,terms:13,products:89},
    {label:"Meat and seafood",value:.35,terms:13,products:139},
    {label:"Pantry",value:.75,terms:31,products:321},
    {label:"Produce",value:4.58,terms:13,products:70},
    {label:"Snacks and candy",value:2.83,terms:12,products:116}
  ]
};

const items={
  Chicago:{
    cheapest:[["Apples",1.2,1.2],["Tomatoes",2.19,2.79],["Whole milk",1.6,1.5],["Eggs",4.4,1.98],["Butter",5.49,5.49],["White bread",2.39,2.49],["Ground beef",5,5],["Chicken thighs",1.99,1.99],["Black beans",1.46,1.46],["Frozen broccoli",1.68,1.68],["Cereal",2.66,2],["Orange juice",3.65,3.65]],
    median:[["Apples",1.66,2.26],["Tomatoes",6.38,5.32],["Whole milk",5.99,5.85],["Eggs",5.69,6.49],["Butter",9.98,6.79],["White bread",4.65,4.65],["Ground beef",7.29,6.99],["Chicken thighs",4.99,4.99],["Black beans",1.99,1.99],["Frozen broccoli",2.69,2.69],["Cereal",5.59,6.09],["Orange juice",5.25,5.41]]
  },
  Dallas:{
    cheapest:[["Apples",1,1],["Tomatoes",2.19,2.49],["Whole milk",3.75,3.75],["Eggs",5.66,5.66],["Butter",3.79,3.49],["White bread",1.49,1.59],["Ground beef",4.6,4],["Chicken thighs",1.99,1.99],["Black beans",1.54,1.54],["Frozen broccoli",1.45,1.45],["Cereal",4.29,3.99],["Orange juice",3.65,3.65]],
    median:[["Apples",1,1.66],["Tomatoes",5.13,4.78],["Whole milk",5.29,5.56],["Eggs",6.66,4.99],["Butter",4.99,5.99],["White bread",3.91,3.91],["Ground beef",6.66,7.25],["Chicken thighs",2.99,4.99],["Black beans",.98,1.51],["Frozen broccoli",2.92,2.54],["Cereal",4.2,4.09],["Orange juice",5.64,5.41]]
  },
  "Los Angeles":{
    cheapest:[["Apples",1,1],["Tomatoes",2.99,2.99],["Whole milk",2.25,2.5],["Eggs",2.79,2.79],["Butter",4.79,4.29],["White bread",1.99,1.99],["Ground beef",6.66,6.6],["Chicken thighs",2.99,2.99],["Black beans",1.25,1.29],["Frozen broccoli",1.31,1.38],["Cereal",1.99,1.99],["Orange juice",3.57,3.57]],
    median:[["Apples",1.66,2.15],["Tomatoes",5.81,6.06],["Whole milk",6.49,5.99],["Eggs",5.99,6.49],["Butter",8.49,6.99],["White bread",4.49,4.54],["Ground beef",9.49,8.49],["Chicken thighs",6.67,7.49],["Black beans",1.59,1.93],["Frozen broccoli",3.21,3.19],["Cereal",4.79,4.79],["Orange juice",5.64,5.64]]
  },
  Seattle:{
    cheapest:[["Apples",1,1],["Tomatoes",2.99,3.49],["Whole milk",2.3,2],["Eggs",3.99,5.49],["Butter",5.99,5.99],["White bread",2.29,2.29],["Ground beef",8.99,9.99],["Chicken thighs",3.99,3.99],["Black beans",1.25,1.25],["Frozen broccoli",1.99,1.99],["Cereal",1.53,1.53],["Orange juice",3.65,3.65]],
    median:[["Apples",1.66,2.33],["Tomatoes",6.38,6.38],["Whole milk",5.79,5.79],["Eggs",5.99,6.79],["Butter",8.49,6.99],["White bread",5.82,5.41],["Ground beef",10.99,10.99],["Chicken thighs",7.63,6.67],["Black beans",1.99,1.99],["Frozen broccoli",3.37,3.37],["Cereal",4.61,4.9],["Orange juice",6.21,5.84]]
  }
};

const findings={
  median:{
    Chicago:"Chicago's median basket fell $2.64, led by cheaper butter and tomatoes despite higher eggs, apples and cereal.",
    Dallas:"Dallas's median basket rose $2.31. Chicken thighs and butter drove the increase, despite eggs falling $1.67 per dozen.",
    "Los Angeles":"Los Angeles's median basket fell $0.58. Cheaper butter, beef and milk offset higher chicken, eggs and apples.",
    Seattle:"Seattle's median basket fell $1.48. Cheaper butter, chicken thighs, juice and bread outweighed higher eggs and apples."
  },
  cheapest:{
    Chicago:"Chicago's fixed bargain basket fell $2.48, led by eggs and cereal; the exact same products and package quantities are tracked in both snapshots.",
    Dallas:"Dallas's fixed bargain basket fell $0.80, driven mainly by the same five-pound ground-beef roll and butter SKU getting cheaper.",
    "Los Angeles":"Los Angeles's fixed bargain basket was nearly flat at −$0.20, with the same products and package quantities in both snapshots.",
    Seattle:"Seattle's fixed bargain basket rose $2.70, driven by the same egg and ground-beef products becoming more expensive."
  }
};

const productData={
  Chicago:[
    ["Apples",["Kroger® Gala Apples – 5 Pound Bag","5 lb",5.99,1.2],["Kroger® Gala Apples – 5 Pound Bag","5 lb",5.99,1.2]],
    ["Tomatoes",["Fresh On the Vine Red Tomatoes by the Bunch","1 lb",2.19,2.19],["Fresh On the Vine Red Tomatoes by the Bunch","1 lb",2.79,2.79]],
    ["Whole milk",["Roundy's® Select Vitamin D Whole Milk","1 gallon",3.19,1.6],["Roundy's® Select Vitamin D Whole Milk","1 gallon",2.99,1.5]],
    ["Eggs",["Kroger® Large White Eggs","60 ct",21.99,4.4],["Kroger® Grade A Large White Eggs","60 ct",9.89,1.98]],
    ["Butter",["Kroger® Salted Butter Sticks","4 sticks / 16 oz",5.49,5.49],["Kroger® Salted Butter Sticks","4 sticks / 16 oz",5.49,5.49]],
    ["White bread",["Kroger® White Bread","20 oz",2.39,2.39],["Kroger® White Bread","20 oz",2.49,2.49]],
    ["Ground beef",["Roundy's Ground Beef 75% Lean 25% Fat","1.2 lb",6,5],["Roundy's Ground Beef 75% Lean 25% Fat","1.2 lb",6,5]],
    ["Chicken thighs",["Heritage Farm® Bone In Skin On Chicken Thighs","1 lb",1.99,1.99],["Heritage Farm® Bone In Skin On Chicken Thighs","1 lb",1.99,1.99]],
    ["Black beans",["Goya Black Beans","46 oz",4.49,1.46],["Goya Black Beans","46 oz",4.49,1.46]],
    ["Frozen broccoli",["Kroger® Frozen Broccoli Florets BIG DEAL!","52 oz",7.29,1.68],["Kroger® Frozen Broccoli Florets BIG DEAL!","52 oz",7.29,1.68]],
    ["Cereal",["Kroger® Bite Size Frosted Shredded Wheat Cereal","18 oz",3.99,2.66],["Kroger® Bite Size Frosted Shredded Wheat Cereal","18 oz",3,2]],
    ["Orange juice",["Kroger® 100% Orange Juice Pulp Free","1 gal",8.99,3.65],["Kroger® 100% Orange Juice Pulp Free","1 gal",8.99,3.65]]
  ],
  Dallas:[
    ["Apples",["Kroger® Gala Apples – 5 Pound Bag","5 lb",5,1],["Kroger® Gala Apples – 5 Pound Bag","5 lb",5,1]],
    ["Tomatoes",["Fresh On the Vine Red Tomatoes by the Bunch","1 lb",2.19,2.19],["Fresh On the Vine Red Tomatoes by the Bunch","1 lb",2.49,2.49]],
    ["Whole milk",["Simple Truth Organic® Vitamin D Whole Milk Gallon","1 gal",7.49,3.75],["Simple Truth Organic® Vitamin D Whole Milk Gallon","1 gal",7.49,3.75]],
    ["Eggs",["Happy Egg Co® Free Range Large Brown Eggs","18 ct",8.49,5.66],["Happy Egg Co.® Free Range Large Brown Eggs","18 ct",8.49,5.66]],
    ["Butter",["Kroger® Salted Butter Sticks","4 sticks / 16 oz",3.79,3.79],["Kroger® Salted Butter Sticks","4 sticks / 16 oz",3.49,3.49]],
    ["White bread",["Smart Way™ White Bread","20 oz",1.49,1.49],["Smart Way™ White Bread","20 oz",1.59,1.59]],
    ["Ground beef",["Kroger® 73/27 Ground Beef Roll 5 LB","5 lb",22.99,4.6],["Kroger® 73/27 Ground Beef Roll 5 LB","5 lb",20,4]],
    ["Chicken thighs",["Heritage Farm® Bone In Skin On Chicken Thighs","1 lb",1.99,1.99],["Heritage Farm® Bone In Skin On Chicken Thighs","1 lb",1.99,1.99]],
    ["Black beans",["Goya® Black Beans","15.5 oz",1.59,1.54],["Goya® Black Beans","15.5 oz",1.59,1.54]],
    ["Frozen broccoli",["Kroger® Frozen Broccoli Florets BIG DEAL!","52 oz",6.29,1.45],["Kroger® Frozen Broccoli Florets BIG DEAL!","52 oz",6.29,1.45]],
    ["Cereal",["General Mills Cinnamon Toast Crunch™ Cereal","12 oz",4.29,4.29],["General Mills Cinnamon Toast Crunch™ Cereal","12 oz",3.99,3.99]],
    ["Orange juice",["Kroger® 100% Orange Juice Pulp Free","half gal",4.49,3.65],["Kroger® 100% Orange Juice Pulp Free","half gal",4.49,3.65]]
  ],
  "Los Angeles":[
    ["Apples",["Kroger® Pink Lady® Apples – 5 Pound Bag BIG DEAL!","5 lb",5,1],["Kroger® Pink Lady® Apples – 5 Pound Bag BIG DEAL!","5 lb",5,1]],
    ["Tomatoes",["Simple Truth Organic® Roma Tomatoes","16 oz",2.99,2.99],["Simple Truth Organic® Roma Tomatoes","16 oz",2.99,2.99]],
    ["Whole milk",["Ralphs® Vitamin D Whole Milk Gallon","1 gal",4.49,2.25],["Ralphs® Vitamin D Whole Milk Gallon","1 gal",4.99,2.5]],
    ["Eggs",["Kroger® Cage Free Medium White Eggs","12 ct",2.79,2.79],["Kroger® Cage Free Medium White Eggs","12 ct",2.79,2.79]],
    ["Butter",["Kroger® Salted Butter Sticks","4 sticks / 16 oz",4.79,4.79],["Kroger® Salted Butter Sticks","4 sticks / 16 oz",4.29,4.29]],
    ["White bread",["Kroger® White Bread","20 oz",1.99,1.99],["Kroger® White Bread","20 oz",1.99,1.99]],
    ["Ground beef",["Kroger® 80/20 Ground Beef Roll 3 LB","3 lb",19.99,6.66],["Kroger® 80/20 Ground Beef Roll 3 LB (Limit 2 at sale price)","3 lb",19.79,6.6]],
    ["Chicken thighs",["Foster Farms Fresh & Natural Chicken Thighs Value Pack","1 lb",2.99,2.99],["Foster Farms Fresh & Natural Chicken Thighs Family Pack","1 lb",2.99,2.99]],
    ["Black beans",["Sun Vista® Black Beans","30 oz",2.49,1.25],["Sun Vista® Black Beans","30 oz",2.59,1.29]],
    ["Frozen broccoli",["Kroger® Traditional Favorites Frozen Broccoli Cuts","32 oz",3.49,1.31],["Kroger® Traditional Favorites Frozen Broccoli Cuts","32 oz",3.69,1.38]],
    ["Cereal",["Kroger® Bite Size Frosted Shredded Wheat Cereal","18 oz",2.99,1.99],["Kroger® Bite Size Frosted Shredded Wheat Cereal","18 oz",2.99,1.99]],
    ["Orange juice",["Kroger® 100% Orange Juice Pulp Free","1 gal",8.79,3.57],["Kroger® 100% Orange Juice Pulp Free","1 gal",8.79,3.57]]
  ],
  Seattle:[
    ["Apples",["Kroger® Gala Apples – 5 Pound Bag","5 lb",5,1],["Kroger® Gala Apples – 5 Pound Bag","5 lb",5,1]],
    ["Tomatoes",["Fresh On the Vine Red Tomatoes by the Bunch","1 lb",2.99,2.99],["Fresh On the Vine Red Tomatoes by the Bunch","1 lb",3.49,3.49]],
    ["Whole milk",["QFC Vitamin D Whole Milk Gallon","1 gal",4.59,2.3],["QFC Vitamin D Whole Milk Gallon","1 gal",3.99,2]],
    ["Eggs",["Eggland's Best Cage Free Large White Eggs, 12 count","12 ct",3.99,3.99],["Eggland's Best Cage Free Large White Eggs","12 ct",5.49,5.49]],
    ["Butter",["Darigold Natural Salted Butter Sticks","4 sticks / 16 oz",5.99,5.99],["Darigold Natural Salted Butter Sticks","4 sticks / 16 oz",5.99,5.99]],
    ["White bread",["Kroger® White Bread","20 oz",2.29,2.29],["Kroger® White Bread","20 oz",2.29,2.29]],
    ["Ground beef",["Wendy’s Ground Beef Patties (Exclusive To Kroger!)","16 oz",8.99,8.99],["Wendy’s Ground Beef Patties (Exclusive To Kroger!)","16 oz",9.99,9.99]],
    ["Chicken thighs",["DRAPER VALLEY FARMS® Bone-In Chicken Thighs","1 lb",3.99,3.99],["DRAPER VALLEY FARMS® Bone-In Chicken Thighs","1 lb",3.99,3.99]],
    ["Black beans",["Kroger No Salt Black Beans - 15.5oz can","15.5 oz",1.29,1.25],["Kroger No Salt Black Beans - 15.5oz can","15.5 oz",1.29,1.25]],
    ["Frozen broccoli",["Kroger® Traditional Favorites Frozen Broccoli Florets","12 oz",1.99,1.99],["Kroger® Frozen Broccoli Florets","12 oz",1.99,1.99]],
    ["Cereal",["Kroger® Bite Size Frosted Shredded Wheat Cereal","18 oz",2.29,1.53],["Kroger® Bite Size Frosted Shredded Wheat Cereal","18 oz",2.29,1.53]],
    ["Orange juice",["Kroger® 100% Orange Juice Pulp Free","1 gal",8.99,3.65],["Kroger® 100% Orange Juice Pulp Free","1 gal",8.99,3.65]]
  ]
};

let statistic="median",city="Dallas",moverView="items";
const money=n=>`$${n.toFixed(2)}`;
const pct=(a,b)=>100*(b-a)/a;
const signedPct=n=>`${n>0?"+":n<0?"−":""}${Math.abs(n).toFixed(1)}%`;
const escapeHtml=value=>String(value??"").replace(/[&<>"']/g,character=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"}[character]));
const unitLabels={ounce:"per oz",fluid_ounce:"per fl oz",count:"per item"};
const unitMoney=value=>`$${Number(value).toFixed(Number(value)<1?3:2)}`;

function renderCards(){
  document.querySelector("#cityGrid").innerHTML=Object.entries(totals).map(([name,prices],i)=>{
    const active=prices[statistic],p=pct(...active),kind=p>1?"up":p<-1?"down":"flat";
    const rows=["cheapest","median"].map(kindName=>{
      const [before,after]=prices[kindName],change=pct(before,after);
      return `<div class="dual-price ${kindName===statistic?"active":""}">
        <span>${kindName==="cheapest"?"fixed cheapest":kindName}</span><strong>${money(after)}</strong>
        <em class="${change>1?"up-text":change<-1?"down-text":"flat-text"}">${signedPct(change)}</em>
        <small>APR ${money(before)}</small>
      </div>`;
    }).join("");
    return `<article class="city-card ${name===city?"selected":""}" data-city="${name}" tabindex="0">
      <div class="city-label"><span>${name.toUpperCase()}</span><span class="rank">0${i+1}</span></div>
      <div class="dual-prices">${rows}</div>
      <span class="change-pill ${kind}">ACTIVE VIEW · ${signedPct(p)}</span>
    </article>`;
  }).join("");
  document.querySelectorAll(".city-card").forEach(el=>{
    const pick=()=>{city=el.dataset.city;renderAll()};
    el.onclick=pick;el.onkeydown=e=>{if(e.key==="Enter"||e.key===" ")pick()};
  });
}

function renderTabs(){
  document.querySelector("#cityTabs").innerHTML=[...Object.keys(totals),"Four-city median"].map(name=>
    `<button role="tab" aria-selected="${name===city}" class="${name===city?"active":""}" data-city="${name}">${name}</button>`
  ).join("");
  document.querySelectorAll("#cityTabs button").forEach(button=>button.onclick=()=>{city=button.dataset.city;renderAll()});
}

const median=valueSet=>{
  const sorted=[...valueSet].sort((a,b)=>a-b),middle=Math.floor(sorted.length/2);
  return sorted.length%2?sorted[middle]:(sorted[middle-1]+sorted[middle])/2;
};

function cityMoverValues(cityName,view=moverView){
  if(view==="categories")return categoryMovers[cityName];
  const itemChanges=items[cityName][statistic].map(([name,apr,nov])=>({label:name,value:nov-apr}));
  return itemChanges;
}

function moverRows(){
  const marketNames=Object.keys(totals);
  const byMarket=Object.fromEntries(marketNames.map(name=>[name,cityMoverValues(name)]));
  const labels=byMarket[marketNames[0]].map(row=>row.label);
  return labels.map(label=>{
    const marketRows=marketNames.map(name=>byMarket[name].find(row=>row.label===label));
    const changes=marketRows.map(row=>row.value),threshold=moverView==="categories"?.05:.005;
    const selectedRow=city==="Four-city median"?null:byMarket[city].find(row=>row.label===label);
    const selectedValue=selectedRow?selectedRow.value:median(changes);
    const termCounts=marketRows.map(row=>row.terms).filter(Boolean);
    return {
      label,value:selectedValue,
      up:changes.filter(v=>v>threshold).length,
      down:changes.filter(v=>v<-threshold).length,
      flat:changes.filter(v=>Math.abs(v)<=threshold).length,
      terms:selectedRow?.terms,
      products:selectedRow?.products,
      termRange:termCounts.length?`${Math.min(...termCounts)}${Math.min(...termCounts)===Math.max(...termCounts)?"":"–"+Math.max(...termCounts)}`:null,
      allProducts:marketRows.reduce((total,row)=>total+(row.products||0),0)
    };
  }).sort((a,b)=>b.value-a.value);
}

function renderChart(){
  const rows=moverRows(),max=Math.max(...rows.map(row=>Math.abs(row.value)),.01);
  const isCategory=moverView==="categories";
  const strategy=isCategory?"matched-SKU category index":statistic==="cheapest"?"fixed cheapest":"median";
  document.querySelector("#chartTitle").textContent=`${city} · ${strategy} · ${moverView}`;
  document.querySelector("#chartEyebrow").textContent=isCategory?"BROAD CATEGORY INDEX":"ITEM CONTRIBUTIONS";
  document.querySelector("#contributionChart").innerHTML=rows.map(row=>{
    const detail=isCategory
      ? city==="Four-city median"?`${row.termRange} terms/city · ${row.allProducts} SKU matches`:`${row.terms} terms · ${row.products} SKU matches`
      : quantities[row.label];
    const formattedValue=isCategory?signedPct(row.value):`${row.value>0?"+":""}${money(row.value)}`;
    const clickable=isCategory&&city!=="Four-city median";
    const element=clickable?"button":"div";
    const attributes=clickable?` type="button" class="bar-row category-audit-row" data-category="${escapeHtml(row.label)}" aria-label="Inspect ${escapeHtml(row.label)} supporting products in ${escapeHtml(city)}"`:' class="bar-row"';
    return `<${element}${attributes}><span class="bar-label" title="${escapeHtml(row.label)}">${escapeHtml(row.label)}<small>${detail} · ↑${row.up} ↓${row.down} —${row.flat}</small></span>
      <div class="bar-area"><span class="bar ${row.value>=0?"positive":"negative"}" style="width:${Math.abs(row.value)/max*48}%"></span></div>
      <span class="bar-value">${formattedValue}</span></${element}>`;
  }).join("");

  document.querySelector("#categoryAuditHint").hidden=!isCategory||city==="Four-city median";
  document.querySelectorAll(".category-audit-row").forEach(row=>{
    row.onclick=()=>openCategorySupport(row.dataset.category);
  });

  const biggestUp=rows[0],biggestDown=rows[rows.length-1];
  if(isCategory&&city==="Four-city median"){
    document.querySelector("#findingQuote").textContent=`Across four cities, ${biggestUp.label} had the highest median category-index change (${signedPct(biggestUp.value)}), while ${biggestDown.label} had the lowest (${signedPct(biggestDown.value)}). Each search term receives equal weight.`;
  }else if(isCategory){
    document.querySelector("#findingQuote").textContent=`In ${city}, ${biggestUp.label} posted the highest broad category change (${signedPct(biggestUp.value)}), while ${biggestDown.label} posted the lowest (${signedPct(biggestDown.value)}).`;
  }else if(city==="Four-city median"){
    document.querySelector("#findingQuote").textContent=`Across four cities, ${biggestUp.label} had the largest median increase (${biggestUp.value>=0?"+":""}${money(biggestUp.value)}), while ${biggestDown.label} had the largest median decrease (${money(biggestDown.value)}). Direction counts show whether each move was broad or local.`;
  }else{
    document.querySelector("#findingQuote").textContent=findings[statistic][city];
  }
  document.querySelector("#compareProductsButton").hidden=isCategory||statistic!=="cheapest"||city==="Four-city median";
}

function renderProductComparison(){
  document.querySelector("#productsDialogTitle").textContent=`${city}: April vs. November`;
  document.querySelector("#productComparison").innerHTML=productData[city].map(([item,apr,nov])=>{
    const delta=nov[3]-apr[3];
    const same=true;
    const snapshot=(label,data)=>`<div class="snapshot-product">
      <span class="snapshot-date">${label}</span>
      <strong>${data[0]}</strong>
      <span class="product-meta">${data[1]} package · ${money(data[2])} shelf</span>
      <span class="basket-contribution">${money(data[3])}<small>basket price</small></span>
    </div>`;
    return `<article class="product-row">
      <div class="product-item"><span>${item}</span><small>${quantities[item]}</small>${same?'<em>same product ID</em>':''}</div>
      ${snapshot("APR 07",apr)}
      <div class="snapshot-arrow">→</div>
      ${snapshot("NOV 12",nov)}
      <div class="product-delta ${delta>0?"up-text":delta<0?"down-text":"flat-text"}">${delta>0?"+":""}${money(delta)}</div>
    </article>`;
  }).join("");
}

let categorySupportCache;

async function loadCategorySupport(){
  if(categorySupportCache)return categorySupportCache;
  const response=await fetch("category-support.json");
  if(!response.ok)throw new Error(`Category support request failed: ${response.status}`);
  categorySupportCache=await response.json();
  return categorySupportCache;
}

function supportSnapshot(label,data,unit,sku){
  return `<div class="snapshot-product support-snapshot">
    <span class="snapshot-date">${label}</span>
    <strong>${escapeHtml(data.description)}</strong>
    <span class="product-meta">${escapeHtml(data.size||"size unavailable")} · ${money(data.shelfPrice)} shelf</span>
    <span class="support-product-id">SKU ${escapeHtml(sku)}</span>
    <span class="basket-contribution">${unitMoney(data.unitPrice)}<small>${unitLabels[unit]||unit}</small></span>
  </div>`;
}

async function openCategorySupport(category){
  const categoryDialog=document.querySelector("#categorySupportDialog");
  const list=document.querySelector("#categorySupportList");
  document.querySelector("#categorySupportTitle").textContent=`${city} ${category}: April vs. November`;
  list.innerHTML='<p class="support-loading">Loading supporting products…</p>';
  categoryDialog.showModal();

  try{
    const data=await loadCategorySupport();
    const rows=data.filter(row=>row.city===city&&row.category===category);
    const groups=rows.reduce((result,row)=>{
      (result[row.term]??=[]).push(row);return result;
    },{});
    list.innerHTML=Object.entries(groups).map(([term,termRows],index)=>`<details class="support-group"${index===0?" open":""}>
      <summary><span>${escapeHtml(term)}</span><small>${termRows.length} matched SKU${termRows.length===1?"":"s"}</small></summary>
      ${termRows.map(row=>`<article class="category-support-row">
        <div class="support-term"><div><strong>${escapeHtml(unitLabels[row.unit]||row.unit)}</strong><span>SKU ${escapeHtml(row.sku)}</span></div><span class="same-product-badge same">exact SKU match</span></div>
        <div class="support-comparison">
          ${supportSnapshot("APR 07",row.april,row.unit,row.sku)}
          <div class="snapshot-arrow">→</div>
          ${supportSnapshot("NOV 12",row.november,row.unit,row.sku)}
          <div class="product-delta ${row.change>0?"up-text":row.change<0?"down-text":"flat-text"}">${signedPct(row.change)}</div>
        </div>
      </article>`).join("")}</details>`).join("")||'<p class="support-loading">No exact SKU matches were found.</p>';
  }catch(error){
    list.innerHTML=`<p class="support-loading">Could not load category support: ${escapeHtml(error.message)}</p>`;
  }
}

function renderAll(){renderCards();renderTabs();renderChart()}

document.querySelectorAll(".segmented button").forEach(button=>button.onclick=()=>{
  statistic=button.dataset.statistic;
  document.querySelectorAll(".segmented button").forEach(x=>x.classList.toggle("active",x===button));
  renderAll();
});
document.querySelectorAll(".mover-view-toggle button").forEach(button=>button.onclick=()=>{
  moverView=button.dataset.moverView;
  document.querySelectorAll(".mover-view-toggle button").forEach(x=>x.classList.toggle("active",x===button));
  renderChart();
});
const dialog=document.querySelector("#methodologyDialog");
document.querySelector("#methodologyButton").onclick=()=>dialog.showModal();
document.querySelector(".dialog-close").onclick=()=>dialog.close();
dialog.onclick=e=>{if(e.target===dialog)dialog.close()};
const productsDialog=document.querySelector("#productsDialog");
document.querySelector("#compareProductsButton").onclick=()=>{renderProductComparison();productsDialog.showModal()};
document.querySelector(".products-close").onclick=()=>productsDialog.close();
productsDialog.onclick=e=>{if(e.target===productsDialog)productsDialog.close()};
const categorySupportDialog=document.querySelector("#categorySupportDialog");
document.querySelector(".category-support-close").onclick=()=>categorySupportDialog.close();
categorySupportDialog.onclick=e=>{if(e.target===categorySupportDialog)categorySupportDialog.close()};
renderAll();
