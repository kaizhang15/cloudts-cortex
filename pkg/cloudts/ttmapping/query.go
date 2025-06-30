package ttmapping

// GetSeriesByTag 返回包含指定标签编码的时间序列ID
func (m *TTMapping) GetSeriesByTag(tagEnc uint32) []uint64 {
	m.lock.RLock()
	defer m.lock.RUnlock()

	var series []uint64
	for row := 0; row < m.bitmap.Rows; row++ {
		if m.hasTag(row, tagEnc) {
			series = append(series, m.timeseries[row])
		}
	}
	return series
}

// GetTagsBySeries 返回时间序列的所有标签编码
func (m *TTMapping) GetTagsBySeries(seriesID uint64) []uint32 {
	m.lock.RLock()
	defer m.lock.RUnlock()

	row := m.findSeriesRow(seriesID)
	if row == -1 {
		return nil
	}
	return m.getRowTags(row)
}

func (m *TTMapping) hasTag(row int, tagEnc uint32) bool {
	start, end := m.bitmap.Pointers[row], m.bitmap.Pointers[row+1]
	for _, enc := range m.bitmap.Indices[start:end] {
		if enc == tagEnc {
			return true
		}
	}
	return false
}

func (m *TTMapping) findSeriesRow(seriesID uint64) int {
	for i, id := range m.timeseries {
		if id == seriesID {
			return i
		}
	}
	return -1
}

func (m *TTMapping) getRowTags(row int) []uint32 {
	start, end := m.bitmap.Pointers[row], m.bitmap.Pointers[row+1]
	return m.bitmap.Indices[start:end]
}