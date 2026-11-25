package chunkstore

import "context"

// DialBCatalog gives master creation and blockvolume attachment the same
// single-filer check, storage rule, and catalog path.
func DialBCatalog(ctx context.Context, cfg SWFSConfig) (*BCatalog, *SWFS, error) {
	client, err := DialSWFS(cfg)
	if err != nil {
		return nil, nil, err
	}
	if err := client.CheckSingleFiler(ctx); err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	cat, err := NewBCatalog(client, client, cfg.AssignPath)
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	return cat, client, nil
}
