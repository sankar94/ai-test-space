// js/app.js
document.addEventListener('DOMContentLoaded', function() {
    const DEFAULT_CENTER = [13.0827, 80.2707];
    const DEFAULT_ZOOM = 12;

    const map = L.map('map').setView(DEFAULT_CENTER, DEFAULT_ZOOM);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 19,
        attribution: '© OpenStreetMap'
    }).addTo(map);

    let floodLayer = L.layerGroup().addTo(map);
    let crimeLayer = L.layerGroup().addTo(map);
    let incidentLayer = L.layerGroup().addTo(map);

    let userReports = { type: "FeatureCollection", features: [] };

    const statusEl = document.getElementById('status');
    const reportModal = document.getElementById('report-modal');
    const detailModal = document.getElementById('detail-modal');
    const searchResults = document.getElementById('search-results');
    const resultsList = document.getElementById('results-list');
    const resultCount = document.getElementById('result-count');
    const filterToggle = document.getElementById('filter-toggle');
    const filterDropdown = document.getElementById('filter-dropdown');

    let reportingMode = false;
    let tempMarker = null;
    let selectedLocation = null;
    let uploadedImages = [];
    let currentMarkers = {};

    const crimeIcon = L.divIcon({
        html: '<div style="background: #dc3545; width: 30px; height: 30px; border-radius: 50%; border: 3px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);"></div>',
        iconSize: [30, 30], className: ''
    });

    const incidentIcon = L.divIcon({
        html: '<div style="background: #ffc107; width: 30px; height: 30px; border-radius: 50%; border: 3px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);"></div>',
        iconSize: [30, 30], className: ''
    });

    const tempIcon = L.divIcon({
        html: '<div style="background: #28a745; width: 25px; height: 25px; border-radius: 50%; border: 3px solid white; box-shadow: 0 2px 6px rgba(0,0,0,0.4);"></div>',
        iconSize: [25, 25], className: ''
    });

    function setStatus(msg, cls = 'info') {
        statusEl.textContent = msg;
        statusEl.className = `status ${cls}`;
    }

    // Filter Dropdown Toggle
    filterToggle.addEventListener('click', () => {
        const isVisible = filterDropdown.style.display === 'block';
        filterDropdown.style.display = isVisible ? 'none' : 'block';
        filterToggle.textContent = isVisible ? '🔽 Filters' : '🔼 Filters';
    });

    fetch('data/flood_b.geojson')
        .then(response => response.json())
        .then(data => {
            L.geoJSON(data, {
                style: { color: '#3b82f6', weight: 2, fillOpacity: 0.3 },
                onEachFeature: (feature, layer) => {
                    if (feature.properties && feature.properties.name) {
                        layer.bindPopup(`<strong>Flood Area:</strong> ${feature.properties.name}`);
                    }
                }
            }).addTo(floodLayer);
            setStatus('Map loaded', 'ok');
        })
        .catch(() => setStatus('Map ready', 'info'));

    function loadUserReportsFromServer() {
        fetch('/get_reports')
            .then(response => {
                if (!response.ok) throw new Error('Failed');
                return response.json();
            })
            .then(data => {
                userReports = data;
                loadUserReports();
                if (userReports.features.length > 0) {
                    setStatus(`${userReports.features.length} reports loaded`, 'ok');
                }
            })
            .catch(() => setStatus('No saved reports', 'info'));
    }

    function saveToServer() {
        fetch('/save_reports', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(userReports)
        })
        .then(response => response.json())
        .then(() => setStatus('✓ Saved!', 'ok'))
        .catch(() => setStatus('Save failed', 'warn'));
    }

    function loadUserReports() {
        crimeLayer.clearLayers();
        incidentLayer.clearLayers();
        currentMarkers = {};

        const crimeFilters = Array.from(document.querySelectorAll('.crime-filter:checked')).map(cb => cb.value);
        const incidentFilters = Array.from(document.querySelectorAll('.incident-filter:checked')).map(cb => cb.value);

        userReports.features.forEach((feature, index) => {
            const props = feature.properties;
            const coords = feature.geometry.coordinates;
            const latlng = [coords[1], coords[0]];

            if (props.reportType === 'crime' && crimeFilters.includes(props.subType)) {
                const marker = L.marker(latlng, { icon: crimeIcon });
                marker.bindPopup(createPopup(props, index));
                marker.addTo(crimeLayer);
                currentMarkers[index] = marker;
            } else if (props.reportType === 'incident' && incidentFilters.includes(props.subType)) {
                const marker = L.marker(latlng, { icon: incidentIcon });
                marker.bindPopup(createPopup(props, index));
                marker.addTo(incidentLayer);
                currentMarkers[index] = marker;
            }
        });
    }

    function createPopup(props, index) {
        const typeLabel = props.reportType === 'crime' ? 'Crime' : 'Incident';
        const subType = props.subType.replace(/-/g, ' ').replace(/\b\w/g, l => l.toUpperCase());

        return `
            <div class="popup-content">
                <h4>${typeLabel}: ${subType}</h4>
                <p><strong>Description:</strong> ${props.description}</p>
                ${props.vehicleNumber ? `<p><strong>Vehicle:</strong> ${props.vehicleNumber}</p>` : ''}
                <p><strong>Date:</strong> ${new Date(props.timestamp).toLocaleString()}</p>
                <button onclick="viewReportDetail(${index})">View Details</button>
            </div>
        `;
    }

    window.viewReportDetail = function(index) {
        const feature = userReports.features[index];
        const props = feature.properties;
        const coords = feature.geometry.coordinates;

        const typeLabel = props.reportType === 'crime' ? 'Crime' : 'Incident';
        const subType = props.subType.replace(/-/g, ' ').replace(/\b\w/g, l => l.toUpperCase());

        let html = `
            <h3>${typeLabel}: ${subType}</h3>
            <p><strong>Description:</strong> ${props.description}</p>
            <p><strong>Location:</strong> ${coords[1].toFixed(6)}, ${coords[0].toFixed(6)}</p>
            <p><strong>Date:</strong> ${new Date(props.timestamp).toLocaleString()}</p>
            ${props.vehicleNumber ? `<p><strong>Vehicle:</strong> ${props.vehicleNumber}</p>` : ''}
        `;

        if (props.images && props.images.length > 0) {
            html += '<h4>Images:</h4><div class="detail-images">';
            props.images.forEach(img => {
                html += `<img src="${img}" alt="Report" onclick="window.open(this.src)">`;
            });
            html += '</div>';
        }

        document.getElementById('detail-content').innerHTML = html;
        detailModal.style.display = 'block';
    };

    window.goToReport = function(index) {
        const feature = userReports.features[index];
        const coords = feature.geometry.coordinates;
        map.setView([coords[1], coords[0]], 17, { animate: true, duration: 0.5 });
        setTimeout(() => {
            if (currentMarkers[index]) currentMarkers[index].openPopup();
        }, 500);
        setStatus('📍 Location', 'ok');
    };

    document.getElementById('toggle-flood').addEventListener('change', (e) => {
        e.target.checked ? map.addLayer(floodLayer) : map.removeLayer(floodLayer);
    });

    document.getElementById('toggle-crime').addEventListener('change', (e) => {
        e.target.checked ? map.addLayer(crimeLayer) : map.removeLayer(crimeLayer);
    });

    document.getElementById('toggle-incident').addEventListener('change', (e) => {
        e.target.checked ? map.addLayer(incidentLayer) : map.removeLayer(incidentLayer);
    });

    document.querySelectorAll('.crime-filter, .incident-filter').forEach(cb => {
        cb.addEventListener('change', loadUserReports);
    });

    document.getElementById('report-type').addEventListener('change', (e) => {
        document.getElementById('crime-subtypes').style.display = e.target.value === 'crime' ? 'block' : 'none';
        document.getElementById('incident-subtypes').style.display = e.target.value === 'incident' ? 'block' : 'none';
    });

    const ReportControl = L.Control.extend({
        options: { position: 'topleft' },
        onAdd: function() {
            const btn = L.DomUtil.create('button');
            btn.innerHTML = '📍 Report Incident';
            btn.style.cssText = 'background: #28a745; color: white; border: none; padding: 10px 16px; border-radius: 4px; cursor: pointer; font-size: 14px; font-weight: 600; box-shadow: 0 2px 4px rgba(0,0,0,0.2);';
            btn.onclick = () => {
                reportModal.style.display = 'block';
                selectedLocation = null;
                document.getElementById('selected-coords').textContent = '';
            };
            L.DomEvent.disableClickPropagation(btn);
            return btn;
        }
    });
    map.addControl(new ReportControl());

    document.getElementById('use-current-location').addEventListener('click', () => {
        if (navigator.geolocation) {
            navigator.geolocation.getCurrentPosition((position) => {
                selectedLocation = { lat: position.coords.latitude, lng: position.coords.longitude };
                document.getElementById('selected-coords').textContent = `Selected: ${selectedLocation.lat.toFixed(6)}, ${selectedLocation.lng.toFixed(6)}`;
                if (tempMarker) map.removeLayer(tempMarker);
                tempMarker = L.marker([selectedLocation.lat, selectedLocation.lng], { icon: tempIcon }).addTo(map);
                map.setView([selectedLocation.lat, selectedLocation.lng], 16);
            });
        }
    });

    document.getElementById('select-on-map').addEventListener('click', () => {
        reportingMode = true;
        map.getContainer().style.cursor = 'crosshair';
        reportModal.style.display = 'none';
    });

    map.on('click', (e) => {
        if (reportingMode) {
            selectedLocation = e.latlng;
            if (tempMarker) map.removeLayer(tempMarker);
            tempMarker = L.marker([selectedLocation.lat, selectedLocation.lng], { icon: tempIcon }).addTo(map);
            document.getElementById('selected-coords').textContent = `Selected: ${selectedLocation.lat.toFixed(6)}, ${selectedLocation.lng.toFixed(6)}`;
            reportingMode = false;
            map.getContainer().style.cursor = '';
            reportModal.style.display = 'block';
        }
    });

    document.getElementById('report-images').addEventListener('change', (e) => {
        const files = Array.from(e.target.files);
        const preview = document.getElementById('image-preview');
        preview.innerHTML = '';
        uploadedImages = [];
        files.forEach(file => {
            const reader = new FileReader();
            reader.onload = (event) => {
                uploadedImages.push(event.target.result);
                const img = document.createElement('img');
                img.src = event.target.result;
                img.className = 'preview-img';
                preview.appendChild(img);
            };
            reader.readAsDataURL(file);
        });
    });

    document.getElementById('report-form').addEventListener('submit', (e) => {
        e.preventDefault();
        if (!selectedLocation) { alert('Select location'); return; }

        const reportType = document.getElementById('report-type').value;
        const subType = reportType === 'crime' ? document.getElementById('crime-subtype').value : document.getElementById('incident-subtype').value;

        userReports.features.push({
            type: "Feature",
            geometry: { type: "Point", coordinates: [selectedLocation.lng, selectedLocation.lat] },
            properties: {
                reportType, subType,
                description: document.getElementById('report-description').value,
                vehicleNumber: document.getElementById('vehicle-number').value,
                images: uploadedImages,
                timestamp: new Date().toISOString()
            }
        });

        saveToServer();
        document.getElementById('report-form').reset();
        reportModal.style.display = 'none';
        if (tempMarker) map.removeLayer(tempMarker);
        tempMarker = null;
        selectedLocation = null;
        uploadedImages = [];
        document.getElementById('selected-coords').textContent = '';
        document.getElementById('image-preview').innerHTML = '';
        document.getElementById('crime-subtypes').style.display = 'none';
        document.getElementById('incident-subtypes').style.display = 'none';
        loadUserReports();
    });

    document.getElementById('search-button').addEventListener('click', performSearch);
    document.getElementById('search-box').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') performSearch();
    });

    function performSearch() {
        const term = document.getElementById('search-box').value.toLowerCase().trim();
        if (!term) { alert('Enter search term'); return; }

        let found = [];
        userReports.features.forEach((f, i) => {
            const props = f.properties;
            const searchable = `${props.description} ${props.vehicleNumber || ''} ${props.subType} ${props.reportType}`.toLowerCase();
            if (searchable.includes(term)) found.push({ feature: f, index: i });
        });

        if (resultsList) resultsList.innerHTML = '';
        if (resultCount) resultCount.textContent = found.length;

        if (found.length > 0) {
            if (searchResults) searchResults.style.display = 'block';
            found.forEach(item => {
                const props = item.feature.properties;
                const typeLabel = props.reportType === 'crime' ? 'Crime' : 'Incident';
                const subType = props.subType.replace(/-/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                const iconEmoji = props.reportType === 'crime' ? '🚨' : '⚠️';

                const resultItem = document.createElement('div');
                resultItem.className = `result-item ${props.reportType}`;
                resultItem.innerHTML = `
                    <div class="result-icon">${iconEmoji}</div>
                    <div class="result-details">
                        <div class="result-item-header">${typeLabel}: ${subType}</div>
                        <div class="result-item-desc">${props.description}</div>
                        <div class="result-item-meta">
                            ${props.vehicleNumber ? `<span>🚗 ${props.vehicleNumber}</span>` : ''}
                            <span>📅 ${new Date(props.timestamp).toLocaleDateString()}</span>
                        </div>
                    </div>
                `;
                resultItem.onclick = () => goToReport(item.index);
                if (resultsList) resultsList.appendChild(resultItem);
            });
            map.setView([found[0].feature.geometry.coordinates[1], found[0].feature.geometry.coordinates[0]], 14);
        } else {
            if (searchResults) searchResults.style.display = 'block';
            if (resultsList) resultsList.innerHTML = '<div class="no-results">😔 No results</div>';
        }
    }

    document.getElementById('clear-search').addEventListener('click', () => {
        document.getElementById('search-box').value = '';
        if (searchResults) searchResults.style.display = 'none';
        if (resultsList) resultsList.innerHTML = '';
        map.setView(DEFAULT_CENTER, DEFAULT_ZOOM);
    });

    document.querySelector('.close-button').addEventListener('click', () => {
        reportModal.style.display = 'none';
        if (tempMarker) map.removeLayer(tempMarker);
        tempMarker = null;
        reportingMode = false;
        map.getContainer().style.cursor = '';
    });

    document.querySelector('.close-detail').addEventListener('click', () => {
        detailModal.style.display = 'none';
    });

    window.addEventListener('click', (e) => {
        if (e.target === reportModal) {
            reportModal.style.display = 'none';
            if (tempMarker) map.removeLayer(tempMarker);
            tempMarker = null;
            reportingMode = false;
            map.getContainer().style.cursor = '';
        }
        if (e.target === detailModal) detailModal.style.display = 'none';
    });

    loadUserReportsFromServer();
});